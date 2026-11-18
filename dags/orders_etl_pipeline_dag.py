import logging

from datetime import timedelta

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from data_engineering.execution_coordinator import execute_idempotent_pipeline


logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_data_engineering"

INPUT_PATH = "/opt/airflow/etl/data/raw/orders_e2e.csv"
OUTPUT_DIR = "/opt/airflow/output/orders_etl"


@dag(
    dag_id="orders_etl_pipeline",
    start_date=datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "etl", "postgresql"],
)
def orders_etl_pipeline():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def execute_etl() -> str:

        import psycopg

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        airflow_conn = hook.get_connection(
            POSTGRES_CONN_ID
        )

        context = get_current_context()
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        logger.info("Starting ETL pipeline dag_id=%s run_id=%s", dag_id, run_id)

        with psycopg.connect(
            host=airflow_conn.host,
            port=airflow_conn.port or 5432,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password,
        ) as conn:

            try_number = context["ti"].try_number

            logger.info(
                "Airflow task attempt: %s",
                try_number,
            )

            batch_id = execute_idempotent_pipeline(
                conn=conn,
                dag_id=dag_id,
                run_id=run_id,
                input_path=INPUT_PATH,
                output_dir=OUTPUT_DIR,
            )

        logger.info(
            "ETL completed. batch_id=%s",
            batch_id,
        )

        return str(batch_id)

    @task(retries=0)
    def verify_batch(batch_id: str):

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
        )

        result = hook.get_first(
            """
            SELECT status
            FROM etl_batches
            WHERE batch_id = %s
            """,
            parameters=(batch_id,),
        )

        if result is None:
            raise ValueError(
                f"Batch not found: {batch_id}"
            )

        status = result[0]

        if status != "COMPLETED":
            raise RuntimeError(
                f"Batch {batch_id} has status {status}"
            )

        logger.info(
            "Batch verified successfully: %s",
            batch_id,
        )

    batch_id = execute_etl()
    verify_batch(batch_id)


orders_etl_pipeline()