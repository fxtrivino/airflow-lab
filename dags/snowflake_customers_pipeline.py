import pendulum

from airflow.sdk import dag
from common.snowflake_dbt_tasks import create_snowflake_pipeline


@dag(
    dag_id="snowflake_customers_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 11, 1,
        tz="America/Argentina/Buenos_Aires",
    ),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "dbt", "customers", "data-engineering"],
)
def snowflake_customers_pipeline():
    create_snowflake_pipeline("customers")


snowflake_customers_pipeline()