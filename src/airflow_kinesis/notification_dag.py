from airflow.decorators import dag, task
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from datetime import datetime, timedelta, timezone
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
import pandas as pd
import smtplib
from email.message import EmailMessage

from helpers.airflow_conf_args import default_args_test
from helpers.mongo import MongoConnect
from salina_kpi_dags.helpers.constants import *


def send_email(subject, body) -> None:
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
   
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(FROM_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)
            print("Email sent successfully.")
    except Exception as e:
        print(f"Email not sent: {e}")



@dag(
    dag_id = "salina_alert_notification",
    start_date = datetime(2024, 1, 1),
    schedule = None,
    catchup = False,
    default_args = default_args_test,
    description = "",
    tags = ["syncer", "salina"],
)
def main() -> None:
    
    @task()
    def syncer_task(mongo_db: str, slack_conn: str) -> None:
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", None)

        mongo = MongoConnect(database=mongo_db)
        hook = SlackWebhookHook(slack_webhook_conn_id=slack_conn)

        # now = datetime.now(timezone.utc)

        users = mongo.find(
            collection="users",
            data={
                "activity_status": "active"
            },
            project={
                "_id": 0,
                "user_id": 1,
                "username": 1
            }
        )

        for user in users:
            print("PROCESSING", user["username"])

            docs = mongo.find(
                collection="sma",
                data={
                    "user_id": ObjectId(user.get("user_id", None))
                }
            )
            docs = list(docs)
            if not docs:
                print("No SMA data for", user["username"])
                continue

            df = pd.DataFrame(docs).sort_values(by="request_datetime", ascending=True).round(4)
            df["request_datetime"] = pd.to_datetime(df["request_datetime"])
            # print(df)

            last_row = df.iloc[-1]
            previous_row = df[df["request_datetime"] == last_row["request_datetime"] - pd.Timedelta(hours=1)]

            if not previous_row.empty:
                last_row_moving_avg = last_row["moving_average"]
                previous_row_upper_bound = previous_row.iloc[0]["upper_bound"]
                previous_row_lower_bound = previous_row.iloc[0]["lower_bound"]
                previous_row_datetime = pd.to_datetime(previous_row.iloc[0]["request_datetime"])

                try:
                    utc_offset = timedelta(hours=8)

                    last_row = last_row.copy()
                    previous_row = previous_row.copy()

                    last_row["request_datetime"] = last_row["request_datetime"] + utc_offset
                    previous_row_datetime = previous_row_datetime + utc_offset
                except Exception as e:
                    print(e)

                if last_row_moving_avg == 0 or previous_row_upper_bound == 0 or previous_row_lower_bound == 0:
                    continue
                elif last_row_moving_avg > previous_row_upper_bound:
                    print(f"-- SPIKE {last_row_moving_avg} - {previous_row_upper_bound} =", last_row_moving_avg - previous_row_upper_bound)

                    try:
                        mongo.store(
                            collection="token_alerts",
                            data={
                                "user_id": ObjectId(user.get("user_id", None)),
                                "username": user.get("username", None),
                                "prev_hour_upper_bound": previous_row_upper_bound,
                                "recent_avg": last_row_moving_avg,
                                "date_created": last_row["request_datetime"],
                            }
                        )

                        hook.send_text(
                            f"```\n"
                            f"{last_row['request_datetime']} -  Spike in Token Cost\n\n"
                            f"Username:\t{user['username']}\n"
                            f"[{previous_row_datetime}] Prev. Hour Cost Upper Bound:\t${previous_row_upper_bound}\n"
                            f"[{last_row['request_datetime']}] Recent Average Cost:\t\t\t${last_row_moving_avg}"
                            f"\n```"
                        )  

                        subject = f"Spike in Token Cost [{last_row['request_datetime']}] - {user['username']}"
                        body =  f"{last_row['request_datetime']} -  Spike in Token Cost\n\n" \
                                f"Username:\t{user['username']}\n" \
                                f"[{previous_row_datetime}] Prev. Hour Cost Upper Bound:\t${previous_row_upper_bound}\n" \
                                f"[{last_row['request_datetime']}] Recent Average Cost:\t\t\t${last_row_moving_avg}"
                        
                        send_email(subject, body)

                    except DuplicateKeyError as e:
                        print(f"Duplicate key error {user['username']}", e)

                elif last_row_moving_avg < previous_row_lower_bound:
                    print(f"-- DROP {last_row_moving_avg} - {previous_row_lower_bound} =", last_row_moving_avg - previous_row_lower_bound)

                    try:
                        mongo.store(
                            collection="token_alerts",
                            data={
                                "user_id": ObjectId(user.get("user_id", None)),
                                "username": user.get("username", None),
                                "prev_hour_lower_bound": previous_row_lower_bound,
                                "recent_avg": last_row_moving_avg,
                                "date_created": last_row["request_datetime"],
                            }
                        )

                        hook.send_text(
                            f"```\n"
                            f"{last_row['request_datetime']} -  Drop in Token Cost\n\n"
                            f"Username:\t{user['username']}\n"
                            f"[{previous_row_datetime}] Prev. Hour Cost Lower Bound:\t${previous_row_lower_bound}\n"
                            f"[{last_row['request_datetime']}] Recent Average Cost:\t\t\t${last_row_moving_avg}"
                            f"\n```"
                        )  

                        subject = f"Drop in Token Cost [{last_row['request_datetime']}] - {user['username']}"
                        body =  f"{last_row['request_datetime']} -  Drop in Token Cost\n\n" \
                                f"Username:\t{user['username']}\n" \
                                f"[{previous_row_datetime}] Prev. Hour Cost Lower Bound:\t${previous_row_lower_bound}\n" \
                                f"[{last_row['request_datetime']}] Recent Average Cost:\t\t\t${last_row_moving_avg}"
                        
                        send_email(subject, body)

                    except DuplicateKeyError as e:
                        print(f"Duplicate key error {user['username']}", e)
                
            else:
                print("Previous row not 1 hour ago")


    t1 = syncer_task.override(task_id="syncer_task_staging")
    t2 = syncer_task.override(task_id="syncer_task_beta")

    t1("salina_kpi_db", "slack_conn") >> t2("salina_kpi_db_beta", "slack_conn_beta")

main()
