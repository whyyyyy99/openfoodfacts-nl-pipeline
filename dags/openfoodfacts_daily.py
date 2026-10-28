"""Daily orchestration for the Open Food Facts delta pipeline."""

from datetime import UTC, date, datetime, timedelta

import pendulum

try:  # Airflow 3 task SDK
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowFailException

from src.airflow_tasks import (
    apply_schema_task,
    detect_source_gap,
    extract_live_deltas,
    load_raw,
    should_full_refresh,
    transform_and_refresh_mart,
)
from src.load import FileLoadError, LoadStateError
from src.transform import DataQualityError


DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Bounds a hung download or lock wait; with max_active_runs=1 an unbounded
    # task would otherwise block every later daily run.
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="openfoodfacts_daily",
    description="Load Open Food Facts deltas and refresh the latest product mart",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    # Trigger with {"full_refresh": true} after a mart-logic change; runs whose
    # logical date is a Sunday also refresh every code as a weekly full check.
    params={"full_refresh": False},
    tags=["openfoodfacts", "data-engineering"],
)
def openfoodfacts_daily():
    @task
    def bootstrap_schema() -> None:
        # Convenience for a clean local checkout, not a schema migration system.
        apply_schema_task()

    @task
    def extract() -> list[str]:
        return extract_live_deltas()

    @task
    def check_source_gap(file_names: list[str]) -> dict[str, object] | None:
        return detect_source_gap(file_names)

    @task
    def load(file_names: list[str]) -> dict[str, int]:
        try:
            return load_raw(file_names)
        except (FileLoadError, LoadStateError) as error:
            # A retry re-reads the same local file and fails the same way; the
            # next DAG run's extract re-validates and re-downloads damaged files.
            raise AirflowFailException(str(error)) from error

    @task
    def transform(params=None, ds=None) -> dict[str, int | bool]:
        # Airflow 3 manual runs may have no logical date; fall back to today.
        run_date = date.fromisoformat(ds) if ds else datetime.now(UTC).date()
        full_refresh = should_full_refresh(
            requested=bool((params or {}).get("full_refresh")),
            run_date=run_date,
        )
        try:
            return transform_and_refresh_mart(full_refresh=full_refresh)
        except DataQualityError as error:
            # Deterministic data failures will not be repaired by task retries.
            raise AirflowFailException(str(error)) from error

    schema_ready = bootstrap_schema()
    selected_files = extract()
    gap_checked = check_source_gap(selected_files)
    raw_loaded = load(selected_files)
    mart_refreshed = transform()

    schema_ready >> selected_files
    gap_checked >> raw_loaded >> mart_refreshed


openfoodfacts_daily()
