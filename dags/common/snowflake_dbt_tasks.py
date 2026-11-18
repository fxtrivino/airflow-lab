import json
import urllib.error
import urllib.request

from datetime import timedelta
from airflow.sdk import task, get_current_context

DBT_RUNNER_URL = "http://dbt-runner:8000"

SUPPORTED_ENTITIES = {
    "customers",
    "orders",
    "payments",
}


def validate_entity(entity: str) -> str:
    entity = entity.lower()

    if entity not in SUPPORTED_ENTITIES:
        raise ValueError(
            f"Unsupported Snowflake entity: {entity}"
        )

    return entity


def request_runner(
    endpoint: str,
    payload: dict | None = None,
    timeout: int = 60,
) -> dict:
    url = f"{DBT_RUNNER_URL}{endpoint}"

    data = None

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=timeout,
        ) as response:
            return json.load(response)

    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            raise RuntimeError(
                "dbt-runner is busy with another job"
            ) from exc

        raise RuntimeError(
            f"dbt-runner returned HTTP {exc.code}"
        ) from exc


def create_snowflake_pipeline(entity: str):
    entity = validate_entity(entity)

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )
    def check_dbt_runner():
        result = request_runner("/health", timeout=10)

        if result.get("status") != "ok":
            raise RuntimeError("dbt-runner is unhealthy")

        return result

    @task(
        retries=2,
        retry_delay=timedelta(seconds=60),
        execution_timeout=timedelta(minutes=10),
    )
    def ingest_batch():
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        filename = conf.get("filename")

        if not filename:
            raise ValueError(
                "Missing filename in dag_run.conf"
            )

        result = request_runner(
            f"/jobs/ingest/{entity}",
            payload={
                "filename": filename,
                "airflow_run_id": context["run_id"],
            },
            timeout=540,
        )

        action = result.get("action")
        status = result.get("status")

        valid_responses = {
            ("INGEST", "RAW_LOADED"),
            ("RESUME_DBT", "RAW_LOADED"),
            ("SKIP", "COMPLETED"),
            ("SKIP", "HISTORICAL_COMPLETED"),
        }

        if (action, status) not in valid_responses:
            raise RuntimeError(
                f"Unexpected ingestion response: {result}"
            )

        if not result.get("batch_id"):
            raise RuntimeError(
                "Ingestion response is missing batch_id"
            )

        if result.get("entity", "").lower() != entity:
            raise RuntimeError(
                f"Unexpected ingestion entity: {result}"
            )

        print(
            f"Entity={entity}, "
            f"batch_id={result['batch_id']}, "
            f"action={action}, "
            f"status={status}"
        )

        return result

    @task(
        retries=2,
        retry_delay=timedelta(seconds=60),
        execution_timeout=timedelta(minutes=35),
    )
    def execute_dbt_build(ingestion_result: dict):
        if ingestion_result["action"] == "SKIP":
            return {
                "status": "SKIPPED",
                "batch_id": ingestion_result["batch_id"],
                "execution_id": None,
                "exit_code": None,
                "metrics": None,
            }

        context = get_current_context()

        result = request_runner(
            "/jobs/dbt-build",
            payload={
                "entity": entity,
                "command": "build",
                "airflow_run_id": context["run_id"],
            },
            timeout=1900,
        )

        if result.get("status") != "COMPLETED":
            raise RuntimeError(
                f"dbt build did not complete: {result}"
            )

        return {
            **result,
            "batch_id": ingestion_result["batch_id"],
        }

    @task
    def validate_dbt_execution(result: dict):
        if result["status"] == "SKIPPED":
            return {
                "status": "SKIPPED",
                "batch_id": result["batch_id"],
            }

        if result["exit_code"] != 0:
            raise RuntimeError("dbt build failed")

        metrics = result["metrics"]

        if (
            metrics["error_count"] > 0
            or metrics["fail_count"] > 0
            or metrics["skipped_count"] > 0
            or metrics["total_nodes"] == 0
        ):
            raise RuntimeError(
                f"dbt validation failed: {metrics}"
            )

        return {
            "status": "PASSED",
            "batch_id": result["batch_id"],
            "execution_id": result["execution_id"],
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )
    def finalize_batch(validation_result: dict):
        if validation_result["status"] == "SKIPPED":
            return validation_result

        result = request_runner(
            "/jobs/finalize-batch",
            payload={
                "batch_id": validation_result["batch_id"],
                "dbt_execution_id": validation_result["execution_id"],
            },
            timeout=60,
        )

        if result.get("status") != "COMPLETED":
            raise RuntimeError(
                f"Batch finalization failed: {result}"
            )

        return result

    runner = check_dbt_runner()
    ingestion = ingest_batch()
    build = execute_dbt_build(ingestion)
    validation = validate_dbt_execution(build)
    finalization = finalize_batch(validation)

    runner >> ingestion

    return finalization