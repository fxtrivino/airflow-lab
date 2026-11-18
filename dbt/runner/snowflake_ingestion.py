import os
import re
from pathlib import Path

import snowflake.connector
import yaml
import hashlib
import uuid
import csv
from entity_config import EntityConfig, get_entity_config

AUDIT_TABLE = "DATA_ENGINEERING_LAB.RAW.ETL_BATCHES"
DATABASE = "DATA_ENGINEERING_LAB"
SCHEMA = "RAW"
STAGE = "CUSTOMERS_STAGE"
TABLE = "CUSTOMERS_RAW"

DATA_DIR = Path("/opt/dbt/data/customers")

FILENAME_PATTERN = re.compile(r"customers_batch_[0-9]+\.csv")


def get_connection():
    profiles_dir = Path(
        os.environ.get("DBT_PROFILES_DIR", "/opt/dbt/profiles")
    )

    with (profiles_dir / "profiles.yml").open(encoding="utf-8") as file:
        profiles = yaml.safe_load(file)

    profile = profiles["snowflake_data_platform"]
    config = profile["outputs"][profile["target"]]

    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=SCHEMA,
        private_key_file=config["private_key_path"],
        private_key_file_pwd=os.environ["DBT_PRIVATE_KEY_PASSPHRASE"],
        authenticator="SNOWFLAKE_JWT",
    )

def calculate_sha256(file_path: Path) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            sha256.update(chunk)

    return sha256.hexdigest()

def count_csv_rows(file_path: Path) -> int:
    with file_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.reader(file)

        header = next(reader, None)

        if header is None:
            raise ValueError("CSV file is empty")

        return sum(1 for row in reader if row)

def validate_recovery_source(
    batch: dict,
    rows_loaded: int,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    filename = batch["source_file"]

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    file_path = config.data_dir / filename

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    current_sha256 = calculate_sha256(file_path)
    expected_sha256 = batch["input_sha256"]

    if current_sha256 != expected_sha256:
        raise RuntimeError(
            f"SHA-256 mismatch for {filename}"
        )

    source_rows = count_csv_rows(file_path)

    if source_rows != rows_loaded:
        raise RuntimeError(
            f"Row count mismatch for {filename}: "
            f"source={source_rows}, "
            f"loaded={rows_loaded}"
        )

    return {
        "filename": filename,
        "sha256": current_sha256,
        "source_rows": source_rows,
        "rows_loaded": rows_loaded,
        "validated": True,
    }

def diagnose_processing_batch(
    cursor,
    batch: dict,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    if batch["status"] != "PROCESSING":
        raise ValueError(
            f"Expected PROCESSING, got {batch['status']}"
        )

    inspection = inspect_processing_batch(
        cursor,
        batch,
        config,
    )

    if inspection["action"] != "RECOVERY_CANDIDATE":
        return inspection

    try:
        validation = validate_recovery_source(
            batch,
            rows_loaded=inspection["rows_loaded"],
            config=config,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        return {
            **inspection,
            "action": "MANUAL_REVIEW",
            "reason": str(exc),
        }

    return {
        **inspection,
        "action": "READY_FOR_RECONCILIATION",
        "source_validation": validation,
    }

def find_batches(
    cursor,
    filename: str,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    cursor.execute(
        f"""
        SELECT
            BATCH_ID,
            SOURCE_FILE,
            INPUT_SHA256,
            STATUS,
            ROWS_LOADED
        FROM {AUDIT_TABLE}
        WHERE SOURCE_FILE = %s
        ORDER BY CREATED_AT DESC
        """,
        (filename,),
    )

    return [
        {
            "batch_id": row[0],
            "source_file": row[1],
            "input_sha256": row[2],
            "status": row[3],
            "rows_loaded": row[4],
        }
        for row in cursor.fetchall()
    ]

def validate_batch_history(
    batches: list[dict],
    sha256: str,
):
    for batch in batches:
        if batch["input_sha256"] != sha256:
            raise ValueError(
                "File content conflict: "
                f"{batch['source_file']} "
                "was previously registered with a different SHA-256"
            )

    return batches[0] if batches else None

def find_copy_history(
    cursor,
    filename: str,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    cursor.execute(
        f"""
        WITH copy_history AS (
            SELECT
                FILE_NAME,
                STATUS,
                ROW_COUNT,
                ROW_PARSED,
                LAST_LOAD_TIME
            FROM TABLE(
                {DATABASE}.INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => '{DATABASE}.{SCHEMA}.{config.table}',
                    START_TIME => DATEADD(
                        'day',
                        -7,
                        CURRENT_TIMESTAMP()
                    )
                )
            )
        )
        SELECT
            FILE_NAME,
            STATUS,
            ROW_COUNT,
            ROW_PARSED,
            LAST_LOAD_TIME
        FROM copy_history
        WHERE FILE_NAME = %s
        ORDER BY LAST_LOAD_TIME DESC
        LIMIT 1
        """,
        (filename,),
    )

    row = cursor.fetchone()

    if row is None:
        return None

    return {
        "filename": row[0],
        "status": row[1],
        "rows_loaded": row[2],
        "rows_parsed": row[3],
        "loaded_at": str(row[4]),
    }

def find_staged_file(
    cursor,
    filename: str,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    cursor.execute(
        f"""
        LIST @{DATABASE}.{SCHEMA}.{config.stage}
        PATTERN = '.*{re.escape(filename)}'
        """
    )

    rows = cursor.fetchall()

    if not rows:
        return None

    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one staged file for {filename}, "
            f"found {len(rows)}"
        )

    row = rows[0]

    return {
        "filename": filename,
        "stage_path": row[0],
        "size": row[1],
        "md5": row[2],
        "last_modified": str(row[3]),
    }

def inspect_processing_batch(
    cursor,
    batch: dict,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    filename = batch["source_file"]

    history = find_copy_history(
        cursor,
        filename,
        config,
    )

    staged_file = find_staged_file(
        cursor,
        filename,
        config,
    )

    result = {
        "batch_id": batch["batch_id"],
        "filename": filename,
        "status": batch["status"],
        "copy_history": history,
        "staged_file": staged_file,
    }

    if history is None:
        result.update({
            "action": "MANUAL_REVIEW",
            "reason": (
                "No COPY_HISTORY record found. "
                "Cannot safely determine whether "
                "the file was previously loaded."
            ),
        })
        return result

    if history["status"].upper() != "LOADED":
        result.update({
            "action": "MANUAL_REVIEW",
            "reason": (
                f"Unexpected COPY status: "
                f"{history['status']}"
            ),
        })
        return result

    if staged_file is None:
        result.update({
            "action": "MANUAL_REVIEW",
            "reason": "Loaded file is missing from stage",
        })
        return result

    rows_loaded = int(history["rows_loaded"])

    if rows_loaded <= 0:
        result.update({
            "action": "MANUAL_REVIEW",
            "reason": "COPY_HISTORY reports no loaded rows",
        })
        return result

    result.update({
        "action": "RECOVERY_CANDIDATE",
        "rows_loaded": rows_loaded,
        "reason": (
            "COPY_HISTORY confirms a successful load "
            "and the staged file exists. "
            "File identity must still be verified."
        ),
    })

    return result
    

def check_ingestion_eligibility(
    cursor,
    filename: str,
    sha256: str,
    config: EntityConfig | None = None,
):
    if config is None:
        config = get_entity_config("customers")

    batches = find_batches(
        cursor,
        filename,
        config,
    )

    existing = validate_batch_history(
        batches,
        sha256,
    )

    if existing:
        status = existing["status"]

        if status in ("COMPLETED", "HISTORICAL_COMPLETED"):
            return {
                "action": "SKIP",
                "batch": existing,
            }

        if status == "RAW_LOADED":
            return {
                "action": "RESUME_DBT",
                "batch": existing,
            }

        return {
            "action": "RECONCILE",
            "batch": existing,
        }

    history = find_copy_history(
        cursor,
        filename,
        config,
    )

    if history is not None:
        raise RuntimeError(
            f"{filename} already appears in COPY_HISTORY "
            "but has no audit record. "
            "Manual reconciliation required."
        )

    return {
        "action": "INGEST",
        "batch": None,
    }

def create_batch(
    cursor,
    filename,
    sha256,
    airflow_run_id,
    entity,
):
    batch_id = str(uuid.uuid4())

    cursor.execute(
        f"""
        INSERT INTO {AUDIT_TABLE} (
            BATCH_ID,
            SOURCE_FILE,
            INPUT_SHA256,
            STATUS,
            AIRFLOW_RUN_ID,
            ENTITY_NAME
        )
        VALUES (
            %s, %s, %s,
            'PROCESSING',
            %s, %s
        )
        """,
        (
            batch_id,
            filename,
            sha256,
            airflow_run_id,
            entity.upper(),
        ),
    )

    return batch_id

def mark_raw_loaded(cursor, batch_id, rows_loaded):
    cursor.execute(
        f"""
        UPDATE {AUDIT_TABLE}
        SET
            STATUS = 'RAW_LOADED',
            CURRENT_LAYER = 'RAW',
            ROWS_LOADED = %s,
            RAW_LOADED_AT = CURRENT_TIMESTAMP(),
            UPDATED_AT = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = NULL
        WHERE BATCH_ID = %s
          AND STATUS = 'PROCESSING'
        """,
        (rows_loaded, batch_id),
    )

    if cursor.rowcount != 1:
        raise RuntimeError(
            f"Could not mark batch {batch_id} as RAW_LOADED"
        )

def mark_completed(batch_id: str, dbt_execution_id: str):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {AUDIT_TABLE}
                SET
                    STATUS = 'COMPLETED',
                    CURRENT_LAYER = 'MARTS',
                    DBT_EXECUTION_ID = %s,
                    COMPLETED_AT = CURRENT_TIMESTAMP(),
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = NULL
                WHERE BATCH_ID = %s
                AND STATUS = 'RAW_LOADED'
                """,
                (dbt_execution_id, batch_id),
            )

            if cursor.rowcount == 1:
                connection.commit()

                return {
                    "batch_id": batch_id,
                    "dbt_execution_id": dbt_execution_id,
                    "status": "COMPLETED",
                    "action": "FINALIZED",
                }

            if cursor.rowcount != 0:
                raise RuntimeError(
                    f"Unexpected number of updated rows "
                    f"for batch {batch_id}: {cursor.rowcount}"
                )

            cursor.execute(
                f"""
                SELECT STATUS, DBT_EXECUTION_ID
                FROM {AUDIT_TABLE}
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )

            rows = cursor.fetchall()

            if len(rows) != 1:
                raise RuntimeError(
                    f"Expected exactly one audit record "
                    f"for batch {batch_id}, found {len(rows)}"
                )

            status, existing_execution_id = rows[0]

            if (
                status == "COMPLETED"
                and existing_execution_id == dbt_execution_id
            ):
                return {
                    "batch_id": batch_id,
                    "dbt_execution_id": dbt_execution_id,
                    "status": "COMPLETED",
                    "action": "ALREADY_COMPLETED",
                }

            raise RuntimeError(
                f"Cannot finalize batch {batch_id}: "
                f"status={status}, "
                f"existing_execution_id={existing_execution_id}, "
                f"requested_execution_id={dbt_execution_id}"
            )

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def reconcile_historical_batch(
    entity: str,
    filename: str,
):
    config = get_entity_config(entity)

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    file_path = config.data_dir / filename

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    sha256 = calculate_sha256(file_path)
    source_rows = count_csv_rows(file_path)

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            existing = find_batches(
                cursor,
                filename,
                config,
            )

            if existing:
                raise RuntimeError(
                    f"Batch already registered: {filename}"
                )

            history = find_copy_history(
                cursor,
                filename,
                config,
            )

            if history is None:
                raise RuntimeError(
                    f"No COPY_HISTORY found for {filename}"
                )

            if history["status"].upper() != "LOADED":
                raise RuntimeError(
                    f"Unexpected COPY status: {history['status']}"
                )

            loaded_rows = int(history["rows_loaded"])

            if loaded_rows != source_rows:
                raise RuntimeError(
                    f"Row count mismatch for {filename}: "
                    f"source={source_rows}, loaded={loaded_rows}"
                )

            batch_id = str(uuid.uuid4())

            cursor.execute(
                f"""
                INSERT INTO {AUDIT_TABLE} (
                    BATCH_ID,
                    SOURCE_FILE,
                    INPUT_SHA256,
                    STATUS,
                    ROWS_LOADED,
                    RAW_LOADED_AT,
                    COMPLETED_AT,
                    ENTITY_NAME,
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    'HISTORICAL_COMPLETED',
                    %s,
                    %s,
                    CURRENT_TIMESTAMP(),
                    %s,
                )
                """,
                (
                    batch_id,
                    filename,
                    sha256,
                    loaded_rows,
                    history["loaded_at"],
                    config.name.upper(),
                ),
            )

            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Historical batch registration failed: {filename}"
                )

            connection.commit()

            return {
                "batch_id": batch_id,
                "entity": entity,
                "filename": filename,
                "status": "HISTORICAL_COMPLETED",
                "rows_loaded": loaded_rows,
                "input_sha256": sha256,
            }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def recover_staged_batch(
    entity: str,
    filename: str,
):
    config = get_entity_config(entity)

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid filename: {filename}")

    file_path = config.data_dir / filename

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            batches = find_batches(
                cursor,
                filename,
                config,
            )

            if len(batches) != 1:
                raise RuntimeError(
                    f"Expected one audit record for {filename}, "
                    f"found {len(batches)}"
                )

            batch = batches[0]

            if batch["status"] != "PROCESSING":
                raise RuntimeError(
                    f"Batch is not PROCESSING: "
                    f"{batch['status']}"
                )

            current_sha256 = calculate_sha256(file_path)

            if current_sha256 != batch["input_sha256"]:
                raise RuntimeError(
                    f"SHA-256 mismatch for {filename}"
                )

            source_rows = count_csv_rows(file_path)

            if source_rows <= 0:
                raise RuntimeError(
                    f"Source file has no data rows: {filename}"
                )

            history = find_copy_history(
                cursor,
                filename,
                config,
            )

            if history is not None:
                raise RuntimeError(
                    f"COPY_HISTORY already contains {filename}. "
                    "Manual reconciliation required."
                )

            staged_file = find_staged_file(
                cursor,
                filename,
                config,
            )

            if staged_file is None:
                raise RuntimeError(
                    f"Staged file not found: {filename}"
                )

            # COPY INTO no debe ejecutarse concurrentemente
            # con otra ingesta del mismo archivo.
            copy_sql = f"""
                COPY INTO {DATABASE}.{SCHEMA}.{config.table}
                FROM @{DATABASE}.{SCHEMA}.{config.stage}
                FILES = ('{filename}')
                ON_ERROR = 'ABORT_STATEMENT'
                FORCE = FALSE
            """

            cursor.execute(copy_sql)

            columns = [
                column[0].lower()
                for column in cursor.description
            ]

            copy_rows = [
                dict(zip(columns, row))
                for row in cursor.fetchall()
            ]

            if len(copy_rows) != 1:
                raise RuntimeError(
                    f"Unexpected COPY result: {copy_rows}"
                )

            copy_result = copy_rows[0]

            if copy_result.get("status", "").upper() != "LOADED":
                raise RuntimeError(
                    f"COPY did not load file: {copy_result}"
                )

            rows_loaded = int(
                copy_result.get("rows_loaded", 0)
            )

            if rows_loaded != source_rows:
                raise RuntimeError(
                    f"Row count mismatch: "
                    f"source={source_rows}, "
                    f"loaded={rows_loaded}"
                )

            mark_raw_loaded(
                cursor,
                batch["batch_id"],
                rows_loaded,
            )

            connection.commit()

            return {
                "batch_id": batch["batch_id"],
                "entity": config.name,
                "filename": filename,
                "status": "RAW_LOADED",
                "action": "RECOVERED",
                "rows_loaded": rows_loaded,
                "copy_result": copy_rows,
            }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def ingest_batch(
    entity: str,
    filename: str,
    airflow_run_id: str | None = None,
):
    config = get_entity_config(entity)

    if not config.filename_pattern.fullmatch(filename):
        raise ValueError(f"Invalid batch filename: {filename}")

    file_path = config.data_dir / filename

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    sha256 = calculate_sha256(file_path)

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            eligibility = check_ingestion_eligibility(
                cursor,
                filename,
                sha256,
                config,
            )

            action = eligibility["action"]

            if action in ("SKIP", "RESUME_DBT"):
                batch = eligibility["batch"]

                return {
                    "status": batch["status"],
                    "action": action,
                    "batch_id": batch["batch_id"],
                    "entity": config.name,
                    "filename": filename,
                    "input_sha256": sha256,
                    "rows_loaded": int(batch["rows_loaded"] or 0),
                    "copy_result": [],
                }

            if action == "RECONCILE":
                raise RuntimeError(
                    f"Batch {filename} requires reconciliation "
                    "before retrying ingestion."
                )

            if action != "INGEST":
                raise RuntimeError(
                    f"Unexpected ingestion action: {action}"
                )

            batch_id = create_batch(
                cursor,
                filename,
                sha256,
                airflow_run_id,
                config.name,
            )

            connection.commit()

            put_sql = f"""
                PUT 'file://{file_path}'
                @{DATABASE}.{SCHEMA}.{config.stage}
                AUTO_COMPRESS = FALSE
                OVERWRITE = FALSE
            """

            cursor.execute(put_sql)

            put_columns = [
                column[0].lower()
                for column in cursor.description
            ]

            put_rows = [
                dict(zip(put_columns, row))
                for row in cursor.fetchall()
            ]

            copy_sql = f"""
                COPY INTO {DATABASE}.{SCHEMA}.{config.table}
                FROM @{DATABASE}.{SCHEMA}.{config.stage}
                FILES = ('{filename}')
                ON_ERROR = 'ABORT_STATEMENT'
                FORCE = FALSE
            """

            cursor.execute(copy_sql)

            copy_columns = [
                column[0].lower()
                for column in cursor.description
            ]

            copy_rows = [
                dict(zip(copy_columns, row))
                for row in cursor.fetchall()
            ]

            if len(copy_rows) != 1:
                raise RuntimeError(
                    f"Unexpected COPY INTO result for {filename}: "
                    f"{copy_rows}"
                )

            copy_result = copy_rows[0]

            if copy_result.get("status", "").upper() != "LOADED":
                raise RuntimeError(
                    f"COPY INTO did not load {filename}: "
                    f"{copy_result}"
                )

            rows_loaded = int(
                copy_result.get("rows_loaded", 0)
            )

            mark_raw_loaded(
                cursor,
                batch_id,
                rows_loaded,
            )

            connection.commit()

            return {
                "status": "RAW_LOADED",
                "action": "INGEST",
                "batch_id": batch_id,
                "entity": config.name,
                "filename": filename,
                "input_sha256": sha256,
                "rows_loaded": rows_loaded,
                "put_result": put_rows,
                "copy_result": copy_rows,
            }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

def ingest_customers(
    filename: str,
    airflow_run_id: str | None = None,
):
    return ingest_batch(
        entity="customers",
        filename=filename,
        airflow_run_id=airflow_run_id,
    )