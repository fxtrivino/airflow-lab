import json
import logging
from datetime import timedelta
from airflow.sdk import get_current_context
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pendulum

from airflow.sdk import dag, task


SPARK_RUNNER_URL = "http://spark-etl:8000/run"


@dag(
    dag_id="customers_incremental_etl",
    description="Incremental customers ETL with Spark and Delta Lake",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "delta-lake", "incremental-etl"],
)
def customers_incremental_etl():

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=12),
    )
    def execute_spark_etl():

        context = get_current_context()

        filename = context["dag_run"].conf.get(
            "filename",
            "customers_batch_1.csv",
        )

        try_number = context["ti"].try_number

        payload_data = {
            "filename": filename,
        }

        payload = json.dumps(payload_data).encode("utf-8")

        logging.info(
            "Executing batch=%s, attempt=%s, simulation=%s",
            filename,
            try_number,
            payload_data.get("simulate_failure_after"),
        )

        request = Request(
            url=SPARK_RUNNER_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
            },
            method="POST",
        )

        logging.info("Starting Spark ETL: %s", filename)

        try:
            with urlopen(request, timeout=660) as response:
                result = json.loads(response.read().decode("utf-8"))

        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            logging.error("Spark Runner HTTP %s: %s", exc.code, detail)
            raise

        except URLError:
            logging.exception("Cannot connect to Spark Runner")
            raise

        logging.info("Spark ETL completed: %s", result)

        return {
            "filename": filename,
            "status": result["status"],
        }

    execute_spark_etl()


customers_incremental_etl()