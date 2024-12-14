from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from pymongo.errors import DuplicateKeyError
from bson import ObjectId

from helpers.airflow_conf_args import default_args_test
from helpers.mongo import MongoConnect
from salina_kpi_dags.helpers.models import user_model


@dag(
    dag_id = "salina_users_syncer",
    start_date = datetime(2024, 1, 1),
    schedule = "*/15 * * * *",
    catchup = False,
    default_args = default_args_test,
    description = "",
    tags = ["syncer", "salina"],
)
def main() -> None:
    
    @task()
    def syncer_task(mongo_db_source: str, mongo_db_target: str) -> None:
        mongo_source = MongoConnect(database=mongo_db_source, use_salina=True)
        mongo_target = MongoConnect(database=mongo_db_target)

        docs = mongo_source.find(
            collection="users",
            data={},
            project={
                "_id": 1,
                "email": 1,
                "date_created": 1,
                "date_verified": 1,
                "first_name": 1,
                "last_name": 1,
                "username": 1,
                "is_user_verified": 1,
                "user_status": 1,
                "country": 1,
                "user_account_type": 1
            }
        )

        docs = list(docs)
        num = 0
        num_active = 0
        num_inactive = 0

        for doc in docs:
            user_doc = user_model(
                user_id             = ObjectId(doc.get("_id")),
                email               = doc.get("email"),
                date_created        = doc.get("date_created"),
                date_verified       = doc.get("date_verified"),
                first_name          = doc.get("first_name"),
                last_name           = doc.get("last_name"),
                username            = doc.get("username"),
                is_user_verified    = doc.get("is_user_verified"),
                user_status         = doc.get("user_status"),
                country             = doc.get("country"),
                user_account_type   = doc.get("user_account_type"),
            )

            try:
                mongo_target.store("users", user_doc)
                print("Added", user_doc["user_id"])
                num += 1
            except DuplicateKeyError:
                user_id = doc.get("_id", None)

                recent_activity = mongo_target.find_one(
                    collection="requests",
                    data={
                        "user_id": ObjectId(user_id),
                        "date_updated": {
                            "$gte": datetime.now(timezone.utc) - timedelta(days=7)  # if has activity within the past 7 days = active
                        }
                    }
                )

                if recent_activity:
                    mongo_target.update_one(
                        collection="users",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "activity_status": "active",
                        }
                    )
                    num_active += 1
                else:
                    mongo_target.update_one(
                        collection="users",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "activity_status": "inactive",
                        }
                    )
                    num_inactive += 1
                print("Updated", user_id)
            

        print("Total inserted:", num)
        print("Total active:", num_active)
        print("Total inactive:", num_inactive)

    
    @task()
    def update_user_task(mongo_db_source: str, mongo_db_target: str) -> None:
        mongo_source = MongoConnect(database=mongo_db_source, use_salina=True)
        mongo_target = MongoConnect(database=mongo_db_target)

        data = {
            "date_updated":  {
                "$gte": datetime.now(timezone.utc) - timedelta(minutes=20)
            }
        }

        docs = mongo_source.find(
            collection="users",
            data=data,
            project={
                "_id": 1,
                "username": 1,
            }
        )

        for doc in docs:
            user_id = doc.get("_id")

            user_local_doc = mongo_target.find_one(
                collection="users",
                data={
                    "user_id": ObjectId(user_id)
                },
                project={
                    "username": 1
                }
            ) or {}

            if user_local_doc:
                # change in username
                if user_local_doc.get("username", None) != doc.get("username", None):
                    print("Username changed", user_id)

                    mongo_target.update_one(
                        collection="users",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "username": doc.get("username", None),
                        }
                    )

                    mongo_target.update_many(
                        collection="requests",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "username": doc.get("username", None),
                        }
                    )

                    mongo_target.update_many(
                        collection="request_cost",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "username": doc.get("username", None),
                        }
                    )

                    mongo_target.update_many(
                        collection="sma",
                        query={
                            "user_id": ObjectId(user_id)
                        },
                        data={
                            "username": doc.get("username", None),
                        }
                    )


    trigger_bebot_requests = TriggerDagRunOperator(
        task_id = "trigger_bebot_requests",
        trigger_dag_id = "salina_bebot_request_syncer",
    )

    trigger_buboy_requests = TriggerDagRunOperator(
        task_id = "trigger_buboy_requests",
        trigger_dag_id = "salina_buboy_request_syncer",
    )

    trigger_buboy_agent_creation = TriggerDagRunOperator(
        task_id = "trigger_buboy_agent_creation",
        trigger_dag_id = "salina_buboy_agent_creation_syncer",
    )

    trigger_sieve_requests = TriggerDagRunOperator(
        task_id = "trigger_sieve_requests",
        trigger_dag_id = "sieve_requests_syncer",
    )


    t1_a = syncer_task.override(task_id="syncer_task_staging")
    t1_b = update_user_task.override(task_id="update_user_task_staging")

    t2_a = syncer_task.override(task_id="syncer_task_beta")
    t2_b = update_user_task.override(task_id="update_user_task_beta")


    tasks_chain_1 = t1_a("unified_db_staging", "salina_kpi_db") >> t1_b("unified_db_staging", "salina_kpi_db")
    tasks_chain_2 = t2_a("unified_db_beta", "salina_kpi_db_beta") >> t2_b("unified_db_beta", "salina_kpi_db_beta")
    
    
    (tasks_chain_1 and tasks_chain_2) >> [
        trigger_bebot_requests,
        trigger_buboy_requests,
        trigger_buboy_agent_creation,
        trigger_sieve_requests
    ]

main()
