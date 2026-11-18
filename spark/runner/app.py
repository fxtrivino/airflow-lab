import os
import subprocess
import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Spark ETL Runner")

INPUT_DIR = Path("/opt/spark-etl/data/samples").resolve()
execution_lock = threading.Lock()


class BatchRequest(BaseModel):
    filename: str = Field(min_length=1)
    simulate_failure_after: str | None = None


@app.post("/run")
def run_batch(request: BatchRequest):
    filename = request.filename

    if Path(filename).name != filename or not filename.endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail="Invalid CSV filename",
        )

    input_path = (INPUT_DIR / filename).resolve()

    if not input_path.is_relative_to(INPUT_DIR) or not input_path.is_file():
        raise HTTPException(
            status_code=404,
            detail="CSV file not found",
        )

    if not execution_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="Another Spark job is running",
        )

    try:
        command = [
            "python",
            "-m",
            "incremental_etl.main",
            "--input",
            str(input_path),
        ]

        if request.simulate_failure_after is not None:
            if request.simulate_failure_after not in (
                "customers",
                "quarantine",
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid failure simulation",
                )

            command.extend([
                "--simulate-failure-after",
                request.simulate_failure_after,
            ])

        result = subprocess.run(
            command,
            cwd="/opt/spark-etl",
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )

        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={
                    "returncode": result.returncode,
                    "stderr": result.stderr[-4000:],
                },
            )

        return {
            "status": "success",
            "filename": filename,
            "stdout": result.stdout[-4000:],
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=504,
            detail="Spark execution timed out",
        )
    finally:
        execution_lock.release()