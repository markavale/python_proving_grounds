from airflow.decorators import dag, task
from pendulum import datetime, duration
from scraper_personality_common import (
    WeeklyShift,
    DayShift,
    Schedule,
)
from datetime import datetime


# -------------------- #
# Local module imports #
# -------------------- #

from helpers import airflow_conf_args
from helpers.edmund import dc_scrapy
from helpers.mongo import MongoConnect
from helpers.redis import RedisConnect


"""
SETTINGS
"""
USE_DAY_SHIFT = False
"""
If `True`, the temporal compiler will only be awake in
the morning and afternoon.
"""
STEP = 30
"""
Minute-interval.

Example; STEP = 30

00:00,
00:30,
01:00,
01:30,
02:00,
...
"""
COPIES = 5
"""
How many duplicates per payload.

At `1`, there won't be any duplicates.

Value must be `1` or bigger.
"""
DEDUPE = True
"""
If `True`,
each payload will not have a duplicate per agenda.
"""
INCLUDE_FORBIDDEN_BY_ROBOTS = True
MAX_TASKS = 16
MIN_PAGE_SIZE = 100

QUERY = {
    "priority_number": {
        "$in": [2, 3],
    },
    "country_code": "PHL",
}


@dag(
    dag_id="edmund_dc_scrapy_sections_national",
    start_date=datetime(2024, 1, 1),
    schedule="35,5 * * * *",
    catchup=False,
    default_args={
        "owner": "Michael Edmund Wong",
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "email": airflow_conf_args.default_args["email"],
        "email_on_failure": True,
        "email_on_retry": False,
        "depends_on_past": False,
        "on_failure_callback": airflow_conf_args.task_fail_slack_alert,
    },
    description=f"Publisher for DC Scrapy. Uses temporal schedules.",
    tags=[
        "dc_scrapy",
        "publisher",
        "sections",
        "temporal schedule",
        "dc_mongo",
    ],
)
def main() -> None:
    mongo = MongoConnect(
        database="mmi_scraper_2020",
    )
    redis = RedisConnect().connect()

    # temporal_schedules = mongo.mongo_connection
    # temporal_schedules = temporal_schedules.mmi_scraper_2020  # type: ignore
    # temporal_schedules = temporal_schedules.temporal_schedules

    # day_shift_schedule = DayShift()
    # day_shift_schedule.step = STEP
    # day_shift_schedule = day_shift_schedule.new_schedule()

    # fallback_schedule = WeeklyShift.new(0, 3)
    # fallback_schedule.step = STEP
    # fallback_schedule = fallback_schedule.new_schedule()

    # blends = []

    # if USE_DAY_SHIFT:
    #     blends.append(day_shift_schedule)

    schedule = Schedule()
    schedule.default_weight_mean = 0.5
    schedule.default_weight_range = 1
    schedule.step = STEP
    schedule.agenda_distance_threshold = 999

    @task()
    def task_fn(page, size):
        dc_scrapy.retrieve_and_publish(
            mongo=mongo,
            redis=redis,
            # schedule_selector=lambda website_doc: dc_scrapy.get_temporal_schedule(
            #     temporal_schedules,
            #     fallback_schedule,
            #     website_doc,
            #     blends,
            # ),
            schedule_selector=lambda _: schedule,
            copies=COPIES,
            dedupe=DEDUPE,
            skip=page * size,
            limit=size,
            query=QUERY,
            use_schedule=True,
            include_forbidden_by_robots=INCLUDE_FORBIDDEN_BY_ROBOTS,
        )

    for page, size in dc_scrapy.get_pages(
        mongo=mongo.mongo_connection,
        max_tasks=MAX_TASKS,
        min_page_size=MIN_PAGE_SIZE,
        query=QUERY,
    ):
        _ = task_fn.override(task_id=f"page_{page}")(page, size)


main()



# Below is the referece from the logic above



def retrieve_and_publish(
    mongo: MongoConnect,
    redis: Redis,
    schedule_selector: Callable[[Any], Schedule],
    copies: int,
    dedupe: bool,
    skip: int,
    limit: int,
    query: dict,
    use_schedule: bool = True,
    include_forbidden_by_robots: bool = False,
):
    print("Starting...")

    website_docs = mongo.find(
        collection="websites",
        data=query,
        project={
            "code_snippet": 0,
        },
    )

    if website_docs == None:
        raise Exception("Unable to retrieve websites!")

    print("SKIP:", skip)
    print("LIMIT:", limit)

    website_docs = website_docs.skip(skip).limit(limit)

    total_n = 0

    for website_doc in website_docs:
        schedule = schedule_selector(website_doc)

        if use_schedule:
            if schedule == None:
                continue

            if not schedule.has_agenda_now:
                continue

        data = {
            "website": website_doc["_id"],
        }

        if not include_forbidden_by_robots:
            data["forbidden_by_robots"] = {
                "$ne": True,
            }

        section_docs = mongo.find(
            collection="sections",
            data=data,
            project={
                "temporal_data": 0,
            },
        )

        if use_schedule:
            section_docs = schedule.distribute_now(
                [*section_docs] * copies,
            )

            if section_docs == None:
                continue

            section_docs = section_docs.items

        n = publish(
            section_docs,
            redis,
            website_doc,
            dedupe,
        )
        total_n += n

        if n == 0:
            continue

        print(f"{n:>03}", website_doc["fqdn"])

    print("Total Published:", total_n)
    print("Done.")