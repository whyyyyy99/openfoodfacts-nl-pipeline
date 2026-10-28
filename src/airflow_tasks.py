"""Airflow-callable wrappers around the tested pipeline modules.

This module deliberately does not import Airflow so its functions remain
testable in the project's lightweight Python environment.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import requests

from src.db import get_connection, load_sql
from src.extract import DEFAULT_BASE_URL, default_data_dir, ensure_files, fetch_index
from src.load import run_load
from src.schema import apply_schema
from src.source_files import parse_source_file_name
from src.transform import run_transform


LOGGER = logging.getLogger(__name__)

LOADED_SOURCE_FILES_SQL = load_sql("airflow/loaded_source_files.sql")

# Weekly whole-mart verification: incremental runs only re-check the codes they
# rewrite, so one run a week recomputes and verifies every code.
FULL_REFRESH_WEEKDAY = 6  # Sunday, as date.weekday()


def apply_schema_task() -> None:
    """Bootstrap missing local objects; this is not a migration mechanism."""
    apply_schema()


def extract_live_deltas(
    data_dir: str | Path | None = None,
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    """Ensure every live-index file exists locally and return all filenames.

    The oldest-first result includes files already present on disk, not only
    files downloaded by this call. This lets a later DAG run retry loading a
    file that was downloaded successfully but failed during ingestion.
    """
    destination_dir = Path(data_dir) if data_dir is not None else default_data_dir()
    with requests.Session() as session:
        selected_names = fetch_index(session, base_url)
        paths = ensure_files(
            session,
            destination_dir,
            selected_names,
            base_url=base_url,
        )
    return [path.name for path in paths]


def _loaded_source_file_names() -> list[str]:
    with get_connection() as connection:
        rows = connection.execute(LOADED_SOURCE_FILES_SQL).fetchall()
    return [row[0] for row in rows]


def detect_source_gap(file_names: Iterable[str]) -> dict[str, object] | None:
    """Warn about a potential gap older than the live retention window.

    A positive gap is reported but does not stop later available files from
    loading. An empty database is treated as an initial load, not as a gap.
    """
    available = [(*parse_source_file_name(name), name) for name in file_names]
    if not available:
        raise ValueError("cannot check source continuity without index filenames")

    loaded = []
    ignored_loaded_names = []
    for name in _loaded_source_file_names():
        try:
            start, end = parse_source_file_name(name)
        except ValueError:
            ignored_loaded_names.append(name)
            continue
        loaded.append((start, end, name))

    if ignored_loaded_names:
        LOGGER.warning(
            "Gap check ignored loaded source files with non-window filenames: %s",
            ignored_loaded_names,
        )
    if not loaded:
        if ignored_loaded_names:
            LOGGER.warning(
                "Gap check skipped: no loaded source file has a parseable window"
            )
        else:
            LOGGER.info("Source-gap check skipped for initial load")
        return None

    oldest_start, _, oldest_name = min(available)
    _, latest_end, latest_name = max(
        loaded,
        key=lambda item: (item[1], item[0], item[2]),
    )
    gap_seconds = oldest_start - latest_end
    if gap_seconds <= 0:
        LOGGER.info(
            "Source windows remain recoverable: latest_loaded_window_end=%s, "
            "oldest_available_window_start=%s",
            latest_end,
            oldest_start,
        )
        return None

    warning = {
        "latest_loaded_file": latest_name,
        "latest_loaded_window_end": latest_end,
        "oldest_available_file": oldest_name,
        "oldest_available_window_start": oldest_start,
        "gap_seconds": gap_seconds,
    }
    LOGGER.warning(
        "Potential unrecoverable source-window gap detected: %s",
        warning,
    )
    return warning


def load_raw(
    file_names: Iterable[str], data_dir: str | Path | None = None
) -> dict[str, int]:
    """Load exactly the selected filenames, resolving each below data_dir."""
    destination_dir = Path(data_dir) if data_dir is not None else default_data_dir()
    paths = []
    for file_name in file_names:
        parse_source_file_name(file_name)
        paths.append(destination_dir / file_name)

    results = run_load(paths)
    return {
        "selected_files": len(paths),
        "loaded_files": sum(not result.skipped for result in results),
        "skipped_files": sum(result.skipped for result in results),
        "loaded_rows": sum(
            result.loaded_row_count for result in results if not result.skipped
        ),
    }


def should_full_refresh(*, requested: bool, run_date: date) -> bool:
    """Return whether this run should recompute every mart code."""
    return requested or run_date.weekday() == FULL_REFRESH_WEEKDAY


def transform_and_refresh_mart(
    *, full_refresh: bool = False
) -> dict[str, int | bool]:
    """Stage pending raw records and refresh the product mart."""
    staged_rows, mart_rows = run_transform(full_refresh=full_refresh)
    return {
        "staged_rows": staged_rows,
        "mart_rows": mart_rows,
        "full_refresh": full_refresh,
    }
