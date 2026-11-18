import json
import os
import signal
import subprocess
import threading
import time
import uuid


from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from snowflake_ingestion import (
    ingest_batch,
    ingest_customers,
    mark_completed,
)
from entity_config import ENTITY_CONFIG


app = FastAPI(title="dbt Snowflake Runner")

PROJECT_DIR = Path("/opt/dbt/project")
ARTIFACTS_DIR = Path("/opt/dbt/artifacts")
DBT_TIMEOUT_SECONDS = 1800
DBT_SELECTORS = {
    "customers": "+stg_customers+",
    "orders": "+stg_orders+",
    "payments": "+stg_payments+",
}

execution_lock = threading.Lock()

class FinalizeBatchRequest(BaseModel):
    batch_id: str = Field(
        min_length=36,
        max_length=36,
    )
    dbt_execution_id: str = Field(
        min_length=36,
        max_length=36,
    )

class BuildRequest(BaseModel):
    airflow_run_id: str = Field(
        min_length=1,
        max_length=512,
    )

    entity: str = Field(
        pattern="^(customers|orders|payments)$",
    )

    command: str = Field(
        default="build",
        pattern="^build$",
    )

class IngestionRequest(BaseModel):
    filename: str = Field(
        min_length=1,
        max_length=128,
    )

    airflow_run_id: str = Field(
        min_length=1,
        max_length=512,
    )

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_dbt_results(target_path: Path):
    results_file = target_path / "run_results.json"

    if not results_file.is_file():
        raise RuntimeError(
            "dbt run_results.json not found for this execution"
        )

    with results_file.open(encoding="utf-8") as file:
        data = json.load(file)

    results = data.get("results", [])
    statuses = Counter(
        result.get("status") for result in results
    )

    invocation_id = data.get("metadata", {}).get(
        "invocation_id"
    )

    if not invocation_id:
        raise RuntimeError("Missing dbt invocation_id")

    return {
        "dbt_invocation_id": invocation_id,
        "total_nodes": len(results),
        "success_count": statuses["success"],
        "pass_count": statuses["pass"],
        "error_count": statuses["error"],
        "fail_count": statuses["fail"],
        "skipped_count": statuses["skipped"],
        "elapsed_time": data.get("elapsed_time"),
    }


@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/jobs/finalize-batch")
def finalize_batch(request: FinalizeBatchRequest):
    if not execution_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Another Snowflake job is running",
        )

    try:
        return mark_completed(
            batch_id=request.batch_id,
            dbt_execution_id=request.dbt_execution_id,
        )

    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Batch finalization failed; inspect runner logs",
        )

    finally:
        execution_lock.release()

@app.post("/jobs/ingest/{entity}")
def execute_ingestion(entity: str, request: IngestionRequest):

    entity = entity.lower()

    config = ENTITY_CONFIG.get(entity)

    if config is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported entity: {entity}",
        )

    if not config.filename_pattern.fullmatch(request.filename):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid filename for entity: {entity}",
        )

    if not execution_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Another Snowflake job is running",
        )

    try:
        result = ingest_batch(
            entity=entity,
            filename=request.filename,
            airflow_run_id=request.airflow_run_id,
        )

        action = result["action"]

        if action in ("SKIP", "RESUME_DBT"):
            return result

        if action != "INGEST":
            raise RuntimeError(
                f"Unexpected ingestion action: {action}"
            )

        copy_results = result["copy_result"]

        if not copy_results:
            raise RuntimeError(
                "COPY INTO returned no results"
            )

        for row in copy_results:
            if row["status"] != "LOADED":
                raise RuntimeError(
                    f"Unexpected COPY INTO status: {row}"
                )

            if int(row.get("errors_seen", 0)) != 0:
                raise RuntimeError(
                    f"COPY INTO reported errors: {row}"
                )

        return result

    except HTTPException:
        raise

    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Snowflake ingestion failed; inspect runner logs",
        )

    finally:
        execution_lock.release()


@app.post("/jobs/dbt-build")
def execute_dbt_build(request: BuildRequest):
    if not execution_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Another dbt execution is running",
        )

    execution_id = str(uuid.uuid4())
    target_path = ARTIFACTS_DIR / execution_id

    started_at = utc_now()
    started = time.monotonic()

    try:
        target_path.mkdir(parents=True, exist_ok=False)

        selector = DBT_SELECTORS[request.entity]

        command = [
            "dbt",
            "build",
            "--select",
            selector,
            "--project-dir",
            str(PROJECT_DIR),
            "--target-path",
            str(target_path),
            "--no-use-colors",
        ]

        process = subprocess.Popen(
            command,
            cwd=PROJECT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        try:
            output, _ = process.communicate(
                timeout=DBT_TIMEOUT_SECONDS
            )

        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()

            raise HTTPException(
                status_code=504,
                detail={
                    "message": "dbt execution timeout",
                    "execution_id": execution_id,
                },
            )

        duration = round(time.monotonic() - started, 2)

        metrics = read_dbt_results(target_path)

        result = {
            "execution_id": execution_id,
            "airflow_run_id": request.airflow_run_id,
            "status": (
                "COMPLETED"
                if process.returncode == 0
                else "FAILED"
            ),
            "exit_code": process.returncode,
            "duration_seconds": duration,
            "started_at": started_at,
            "completed_at": utc_now(),
            "metrics": metrics,
        }

        audit_file = target_path / "audit.json"

        with audit_file.open("w", encoding="utf-8") as file:
            json.dump(result, file, indent=2)

        if process.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=result,
            )

        return result

    finally:
        execution_lock.release()