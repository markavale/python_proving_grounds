from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from typing import Callable
from datetime import datetime, timedelta
from bson.json_util import dumps
import json

from helpers.airflow_conf_args import default_args
from helpers.mongo import MongoConnect
from helpers.redis import RedisConnect
from online_section_publishers.helpers.rss_configs import configs


def create_dynamic_dag(dag_id: str, config: dict) -> Callable:
    @dag(
        dag_id = dag_id,
        start_date = datetime(2024, 1, 1),
        schedule = config["schedule"],
        catchup = False,
        default_args = default_args,
        description = "Publishes RSS data from MongoDB to Redis queue.",
        tags = ["online"],
    )
    def main() -> None:
        pool = config["pool"]

        initialize_pool_task = BashOperator(
            task_id = "initialize_pool_task",
            bash_command = f"airflow pools list | grep -q {pool} || airflow pools set {pool} 1 '{pool}'",
        )

        @task(pool=pool, pool_slots=1)
        def publisher_task() -> None:
            mongo_conn = MongoConnect(database="mmi_scraper_2020")
            redis_conn = RedisConnect().connect()


            pipeline = [
                {
                    "$lookup": {
                        "from": "websites",
                        "localField": "website.fqdn",
                        "foreignField": "fqdn",
                        "as": "_website",
                    },
                },
                {
                    "$project": {
                        "_id": 0,
                    }
                },
                {
                    "$unwind": "$_website",
                },
                {
                    "$replaceRoot": {
                        "newRoot": {
                            "$mergeObjects": [
                                "$_website",
                                "$$ROOT",
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "_website": 0,
                    }
                },
            ]

            result = list(mongo_conn.aggregate("rss_feeds", pipeline))
            print(f"Got {len(result)} results.")

            try:
                pipe = redis_conn.pipeline()
                for doc in result:
                    doc = json.loads(dumps(doc))
                    doc["_id"] = str(doc["_id"]["$oid"])
                    pipe.sadd(config["key"], json.dumps(doc))
                pipe.execute()
                print("Done")
            except Exception as e:
                print(e)


        initialize_pool_task >> publisher_task()

    return main


for dag_id, config in configs.items():
    dags = create_dynamic_dag(dag_id, config)
    dags()
