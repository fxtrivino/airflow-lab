import json
import urllib.error
import urllib.request
import pendulum

from unittest import result
from datetime import timedelta

from airflow.sdk import dag, task
from airflow.sdk import get_current_context


DBT_RUNNER_URL = "http://dbt-runner:8000"


@dag(
    dag_id="snowflake_dbt_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 11, 1, tz="America/Argentina/Buenos_Aires"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "dbt", "data-engineering"],
)
def snowflake_dbt_pipeline():

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )
    def ingest_customers_batch():
        context = get_current_context()

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        filename = conf.get("filename")

        if not filename:
            raise ValueError(
                "Missing filename in dag_run.conf"
            )

        payload = json.dumps({
            "filename": filename,
            "airflow_run_id": context["run_id"],
        }).encode("utf-8")

        request = urllib.request.Request(
            f"{DBT_RUNNER_URL}/jobs/ingest-customers",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=540,
            ) as response:
                result = json.load(response)

        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Snowflake ingestion returned HTTP {exc.code}"
            ) from exc

        action = result.get("action")
        status = result.get("status")
        batch_id = result.get("batch_id")

        valid_responses = {
            ("INGEST", "RAW_LOADED"),
            ("RESUME_DBT", "RAW_LOADED"),
            ("SKIP", "COMPLETED"),
        }

        if (action, status) not in valid_responses:
            raise RuntimeError(
                f"Unexpected ingestion response: {result}"
            )

        if not batch_id:
            raise RuntimeError(
                "Ingestion response is missing batch_id"
            )

        print(
            f"Ingestion result: "
            f"batch_id={batch_id}, "
            f"filename={result['filename']}, "
            f"action={action}, "
            f"status={status}, "
            f"rows_loaded={result['rows_loaded']}"
        )

        return result

    @task
    def check_dbt_runner():
        with urllib.request.urlopen(
            f"{DBT_RUNNER_URL}/health",
            timeout=10,
        ) as response:
            result = json.load(response)

        if result.get("status") != "ok":
            raise RuntimeError("dbt-runner is unhealthy")

        return result

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=35),
    )
    def execute_dbt_build(ingestion_result: dict):

        if ingestion_result["action"] == "SKIP":
            print(
                f"Batch {ingestion_result['batch_id']} "
                "already completed. Skipping dbt."
            )

            return {
                "status": "SKIPPED",
                "batch_id": ingestion_result["batch_id"],
                "execution_id": None,
                "exit_code": None,
                "metrics": None,
            }

        context = get_current_context()

        payload = json.dumps({
            "command": "build",
            "airflow_run_id": context["run_id"],
        }).encode("utf-8")

        request = urllib.request.Request(
            f"{DBT_RUNNER_URL}/jobs/dbt-build",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=1900,
            ) as response:
                result = json.load(response)

        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"dbt-runner returned HTTP {exc.code}"
            ) from exc

        if result.get("status") != "COMPLETED":
            raise RuntimeError("dbt build did not complete")

        print(
            f"dbt build completed in "
            f"{result['duration_seconds']} seconds"
        )

        return {
            "status": result["status"],
            "execution_id": result["execution_id"],
            "airflow_run_id": result["airflow_run_id"],
            "duration_seconds": result["duration_seconds"],
            "exit_code": result["exit_code"],
            "metrics": result["metrics"],
            "batch_id": ingestion_result["batch_id"],
        }

    @task
    def validate_dbt_execution(result: dict):

        if result["status"] == "SKIPPED":
            print(
                f"Batch {result['batch_id']} "
                "already completed. Validation skipped."
            )

            return {
                "status": "SKIPPED",
                "batch_id": result["batch_id"],
            }

        if result["exit_code"] != 0:
            raise RuntimeError("dbt build failed")

        metrics = result["metrics"]

        if metrics["error_count"] > 0:
            raise RuntimeError("dbt execution contains errors")

        if metrics["fail_count"] > 0:
            raise RuntimeError("dbt tests failed")

        if metrics["skipped_count"] > 0:
            raise RuntimeError("dbt execution contains skipped nodes")

        if metrics["total_nodes"] == 0:
            raise RuntimeError("dbt execution returned no nodes")

        print(
            json.dumps({
                "execution_id": result["execution_id"],
                "dbt_invocation_id": metrics["dbt_invocation_id"],
                "total_nodes": metrics["total_nodes"],
                "success_count": metrics["success_count"],
                "pass_count": metrics["pass_count"],
                "status": "PASSED",
            }, indent=2)
        )

        return {
            "status": "PASSED",
            "batch_id": result["batch_id"],
            "execution_id": result["execution_id"],
        }

    @task
    def finalize_batch(validation_result: dict):
        if validation_result["status"] == "SKIPPED":
            print("Batch already completed; finalization skipped")
            return validation_result

        payload = json.dumps({
            "batch_id": validation_result["batch_id"],
            "dbt_execution_id": validation_result["execution_id"],
        }).encode("utf-8")

        request = urllib.request.Request(
            f"{DBT_RUNNER_URL}/jobs/finalize-batch",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(
            request,
            timeout=60,
        ) as response:
            result = json.load(response)

        if result.get("status") != "COMPLETED":
            raise RuntimeError(
                f"Batch finalization failed: {result}"
            )

        print(
            f"Batch {result['batch_id']} "
            "successfully finalized"
        )

        return result

    # runner = check_dbt_runner()
    # ingestion = ingest_customers_batch()
    # build = execute_dbt_build()

    # runner >> ingestion >> build
    # validate_dbt_execution(build)

    runner = check_dbt_runner()
    ingestion = ingest_customers_batch()
    build = execute_dbt_build(ingestion)
    validation = validate_dbt_execution(build)
    finalization = finalize_batch(validation)

    runner >> ingestion


snowflake_dbt_pipeline()