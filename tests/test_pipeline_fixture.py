"""End-to-end pipeline runs over the committed tests/fixtures delta files.

These runs use real, unmodified Open Food Facts records and no network, so the
rerun guarantee stays reproducible after the live delta files leave the source.
"""

import pytest

from src.db import get_connection
from src.extract import PROJECT_ROOT
from src.pipeline import run_pipeline


pytestmark = pytest.mark.integration

FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures"
OLDER = "openfoodfacts_products_1792997789_1793079340.json.gz"
NEWER = "openfoodfacts_products_1793079340_1793168139.json.gz"


def _truncate_pipeline_tables():
    with get_connection() as connection:
        database = connection.info.dbname
        assert database.endswith("_test"), f"refusing to truncate {database}"
        connection.execute(
            """
            TRUNCATE mart.refresh_log, mart.products, staging.product_records,
                     raw.product_records, raw.source_files
            """
        )


@pytest.fixture
def empty_database():
    _truncate_pipeline_tables()
    yield
    _truncate_pipeline_tables()


def _run(*file_names, **options):
    return run_pipeline(data_dir=FIXTURE_DIR, file_names=list(file_names), **options)


def test_fixture_run_produces_expected_counts_and_rerun_changes_nothing(empty_database):
    first = _run(OLDER, NEWER, verify=True)

    assert first.selected_files == (OLDER, NEWER)
    assert (first.downloaded_file_count, first.reused_file_count) == (0, 2)
    assert (first.loaded_file_count, first.skipped_file_count) == (2, 0)
    assert first.staged_row_count == 70
    assert first.raw_row_count == first.staging_row_count == 70
    assert (first.valid_staging_row_count, first.invalid_staging_row_count) == (69, 1)
    assert first.non_winning_valid_row_count == 8
    assert first.mart_row_count == 61
    assert first.cross_file_product_count == 8

    second = _run(OLDER, NEWER, verify=True)

    assert (second.loaded_file_count, second.skipped_file_count) == (0, 2)
    assert second.staged_row_count == 0
    assert (second.raw_row_count, second.mart_row_count) == (70, 61)
    assert second.raw_checksum == first.raw_checksum
    assert second.staging_checksum == first.staging_checksum
    assert second.mart_checksum == first.mart_checksum

    with get_connection() as connection:
        refreshes = connection.execute(
            """
            SELECT refresh_mode, affected_product_count, mart_row_count
            FROM mart.refresh_log
            ORDER BY refresh_id
            """
        ).fetchall()
        cross_file_winner_sources = connection.execute(
            """
            SELECT DISTINCT m.source_file_name
            FROM mart.products AS m
            WHERE m.product_code IN (
                SELECT product_code
                FROM staging.product_records
                GROUP BY product_code
                HAVING count(DISTINCT source_file_name) > 1
            )
            """
        ).fetchall()

    assert refreshes == [("mart_incremental", 61, 61), ("mart_incremental", 0, 61)]
    assert cross_file_winner_sources == [(NEWER,)]


def test_older_file_arriving_late_yields_the_same_mart(empty_database):
    together = _run(OLDER, NEWER).mart_checksum
    _truncate_pipeline_tables()

    newer_only = _run(NEWER)
    late = _run(OLDER, NEWER)

    assert newer_only.mart_row_count == 35
    assert (late.loaded_file_count, late.skipped_file_count) == (1, 1)
    assert late.mart_row_count == 61
    assert late.mart_checksum == together


def test_rebuild_reproduces_the_incremental_result(empty_database):
    incremental = _run(OLDER, NEWER)
    rebuilt = _run(OLDER, NEWER, rebuild=True)

    assert rebuilt.staging_checksum == incremental.staging_checksum
    assert rebuilt.mart_checksum == incremental.mart_checksum
