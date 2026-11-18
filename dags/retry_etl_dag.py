from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException


@dag(
    dag_id="retry_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["data-engineering", "retries"],
)
def retry_etl_pipeline():

    @task
    def extract_data():
        print("Extracting data...")
        return [100, 200, 300]

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def transform_data(records, **context):

        try_number = context["ti"].try_number

        print(f"Attempt number: {try_number}")

        if try_number == 1:
            raise AirflowException(
                "Simulated temporary database connection failure"
            )

        return [value * 2 for value in records]

    @task
    def load_data(records):
        print(f"Loading records: {records}")

    extracted = extract_data()
    transformed = transform_data(extracted)
    load_data(transformed)


retry_etl_pipeline()