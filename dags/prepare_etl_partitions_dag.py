import logging
import pendulum
from datetime import datetime, date
from airflow.sdk import dag, task, get_current_context


logger = logging.getLogger(__name__)


@dag(
    dag_id="prepare_etl_partitions",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 10, 1,
        tz="America/Argentina/Buenos_Aires"
    ),
    catchup=False,
    tags=["data-engineering", "postgres", "partitions"],
)
def prepare_etl_partitions():

    @task
    def create_future_partitions():

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        context = get_current_context()

        reference_month = date.fromisoformat(
            context["dag_run"].conf.get(
                "reference_month",
                context["data_interval_end"].date().isoformat()
            )
        )

        logger.info(
            "Reference month: %s",
            reference_month
        )

        hook = PostgresHook(
            postgres_conn_id="postgres_data_engineering"
        )

        sql = """
            SELECT public.create_etl_events_partition(
                (
                    DATE_TRUNC('month', %s::DATE)
                    + make_interval(months => gs)
                )::DATE
            )
            FROM generate_series(1, 3) AS gs;
        """

        hook.run(
            sql,
            parameters=(reference_month,)
        )

        logger.info(
            "Future partitions prepared for reference month: %s",
            reference_month
        )

    create_future_partitions()


prepare_etl_partitions()