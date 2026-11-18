import logging
from datetime import datetime, timedelta
from data_engineering.incident_repository import sync_batch_incidents
import psycopg

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_engineering.batch_monitor import (
    find_stale_batches,
    build_stale_batches_report,
    count_stale_batches,
    has_stale_metrics_changed,
)

from data_engineering.monitor_repository import (
    get_monitor_state,
    save_monitor_state,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="etl_batch_monitor",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "monitoring"],
)
def etl_batch_monitor():

    @task
    def check_stale_batches():
        hook = PostgresHook(
            postgres_conn_id="postgres_data_engineering_monitor"
        )

        connection = hook.get_connection(
            "postgres_data_engineering_monitor"
        )

        with psycopg.connect(
            host=connection.host,
            port=connection.port,
            dbname=connection.schema,
            user=connection.login,
            password=connection.password,
        ) as conn:

            threshold = timedelta(minutes=10)
            monitor_name = "etl_batch_monitor"

            incident_result = sync_batch_incidents(
                conn,
                threshold=threshold,
            )

            logger.info(
                "Incident synchronization: "
                "new=%s ongoing=%s resolved=%s total_open=%s",
                incident_result["new"],
                incident_result["ongoing"],
                incident_result["resolved"],
                incident_result["total_open"],
            )

            # 1. Obtener métricas globales
            metrics = count_stale_batches(
                conn,
                threshold=threshold,
            )

            # 2. Obtener una muestra para diagnóstico
            batches = find_stale_batches(
                conn,
                threshold=threshold,
                limit=100,
            )

            report = build_stale_batches_report(batches)

            # 3. Recuperar el estado anterior
            state = get_monitor_state(conn, monitor_name)

            previous_total = (
                state["last_total"]
                if state is not None
                else None
            )

            previous_by_status = (
                state["last_by_status"]
                if state is not None
                else None
            )

            # 4. Comparar total y distribución
            changed = has_stale_metrics_changed(
                previous_total=previous_total,
                previous_by_status=previous_by_status,
                current_total=metrics["total"],
                current_by_status=metrics["by_status"],
            )

            # 5. Registrar alerta cuando cambien las métricas
            alert_triggered = changed and metrics["total"] > 0

            if alert_triggered:
                logger.warning(
                    "STALE BATCH ALERT: previous_total=%s "
                    "current_total=%s previous_by_status=%s "
                    "current_by_status=%s by_action=%s",
                    previous_total,
                    metrics["total"],
                    previous_by_status,
                    metrics["by_status"],
                    metrics["by_action"],
                )
            else:
                logger.info(
                    "Stale batch monitoring: previous_total=%s "
                    "current_total=%s changed=%s by_status=%s",
                    previous_total,
                    metrics["total"],
                    changed,
                    metrics["by_status"],
                )

            logger.info(
                "Stale batches sample: returned=%s total=%s",
                report["total"],
                metrics["total"],
            )

            # 6. Persistir las métricas actuales
            save_monitor_state(
                conn,
                monitor_name=monitor_name,
                current_total=metrics["total"],
                current_by_status=metrics["by_status"],
                alert_triggered=alert_triggered,
            )

    check_stale_batches()


etl_batch_monitor()