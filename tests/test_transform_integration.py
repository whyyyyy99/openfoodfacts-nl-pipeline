import gzip
import json
from pathlib import Path

import pytest
from psycopg.errors import CheckViolation, LockNotAvailable

from src.airflow_tasks import _loaded_source_file_names
from src.db import get_connection
from src.load import load_file, register_source_file
from src.transform import (
    DataQualityError,
    INVALID_LAST_MODIFIED_T,
    MART_FULL_REFRESH,
    MART_INCREMENTAL,
    MART_INSERT_SQL,
    STAGING_AND_MART_REBUILD,
    assert_layer_reconciliation,
    rebuild_staging_and_mart,
    refresh_mart,
    stage_pending_records,
)
from src.verification import table_checksum


pytestmark = pytest.mark.integration


def _write_delta(path: Path, payload: dict[str, object]) -> Path:
    with gzip.open(path, mode="wt", encoding="utf-8") as target:
        target.write(json.dumps(payload) + "\n")
    return path


def test_invalid_timestamp_and_duplicate_failure_leave_mart_trustworthy():
    connection = get_connection()
    source_file = "test_products_1789689600_1789776000.json.gz"
    try:
        connection.execute(
            """
            INSERT INTO raw.source_files (source_file_name)
            VALUES (%s)
            """,
            (source_file,),
        )
        raw_rows = [
            (1, {"code": "00999", "last_modified_t": 1_789_689_600, "rev": 1}),
            (2, {"code": "00888", "last_modified_t": "not-an-integer", "rev": 1}),
        ]
        for line_number, payload in raw_rows:
            connection.execute(
                """
                INSERT INTO raw.product_records (
                    source_file_name, source_line_number, payload
                ) VALUES (%s, %s, %s::jsonb)
                """,
                (source_file, line_number, json.dumps(payload)),
            )

        assert stage_pending_records(connection) == 2
        invalid = connection.execute(
            """
            SELECT validation_errors, is_valid
            FROM staging.product_records
            WHERE source_file_name = %s AND source_line_number = 2
            """,
            (source_file,),
        ).fetchone()
        assert invalid == ([INVALID_LAST_MODIFIED_T], False)

        assert refresh_mart(connection) >= 1
        refresh_log_before_failure = connection.execute(
            "SELECT count(*) FROM mart.refresh_log"
        ).fetchone()[0]
        assert connection.execute(
            "SELECT count(*) FROM mart.products WHERE product_code = '00888'"
        ).fetchone() == (0,)
        checksum_before_duplicate = table_checksum(connection, "mart")

        duplicate_payload = {
            "code": "00999",
            "last_modified_t": 1_789_689_600,
            "rev": 1,
        }
        connection.execute(
            """
            INSERT INTO raw.product_records (
                source_file_name, source_line_number, payload
            ) VALUES (%s, 3, %s::jsonb)
            """,
            (source_file, json.dumps(duplicate_payload)),
        )
        assert stage_pending_records(connection) == 1

        with pytest.raises(DataQualityError):
            refresh_mart(connection)
        assert table_checksum(connection, "mart") == checksum_before_duplicate
        assert connection.execute(
            "SELECT count(*) FROM mart.refresh_log"
        ).fetchone() == (refresh_log_before_failure,)
    finally:
        connection.rollback()
        connection.close()


def test_staging_streams_across_multiple_database_batches():
    connection = get_connection()
    source_file = "test_products_streaming.json.gz"
    try:
        connection.execute(
            "INSERT INTO raw.source_files (source_file_name) VALUES (%s)",
            (source_file,),
        )
        raw_rows = [
            (
                source_file,
                line_number,
                json.dumps(
                    {
                        "code": f"9{line_number:012d}",
                        "last_modified_t": 1_789_689_600 + line_number,
                    }
                ),
            )
            for line_number in range(1, 206)
        ]
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO raw.product_records (
                    source_file_name, source_line_number, payload
                ) VALUES (%s, %s, %s::jsonb)
                """,
                raw_rows,
            )

        assert stage_pending_records(connection) == 205
        assert connection.execute(
            """
            SELECT count(*)
            FROM staging.product_records
            WHERE source_file_name = %s
            """,
            (source_file,),
        ).fetchone() == (205,)
    finally:
        connection.rollback()
        connection.close()


def test_mart_is_independent_of_source_file_load_order(tmp_path):
    connection = get_connection()
    product_code = "9" * 60
    older = _write_delta(
        tmp_path / "openfoodfacts_products_1900000000_1900000100.json.gz",
        {
            "code": product_code,
            "last_modified_t": 1_900_000_050,
            "rev": 1,
            "product_name": "Older version",
        },
    )
    newer = _write_delta(
        tmp_path / "openfoodfacts_products_1900000100_1900000200.json.gz",
        {
            "code": product_code,
            "last_modified_t": 1_900_000_150,
            "rev": 2,
            "product_name": "Newer version",
        },
    )
    source_names = (older.name, newer.name)

    def load_and_checksum(paths):
        for path in paths:
            register_source_file(connection, path)
            load_file(connection, path)
        assert stage_pending_records(connection) == 2
        refresh_mart(connection)
        winner = connection.execute(
            """
            SELECT product_name, source_file_name
            FROM mart.products
            WHERE product_code = %s
            """,
            (product_code,),
        ).fetchone()
        return winner, table_checksum(connection, "mart")

    try:
        winner_oldest_first, checksum_oldest_first = load_and_checksum([older, newer])

        connection.execute(
            "DELETE FROM mart.products WHERE product_code = %s", (product_code,)
        )
        connection.execute(
            """
            DELETE FROM staging.product_records
            WHERE source_file_name IN (%s, %s)
            """,
            source_names,
        )
        connection.execute(
            """
            DELETE FROM raw.product_records
            WHERE source_file_name IN (%s, %s)
            """,
            source_names,
        )
        connection.execute(
            """
            DELETE FROM raw.source_files
            WHERE source_file_name IN (%s, %s)
            """,
            source_names,
        )

        winner_newest_first, checksum_newest_first = load_and_checksum([newer, older])

        assert winner_oldest_first == ("Newer version", newer.name)
        assert winner_newest_first == winner_oldest_first
        assert checksum_newest_first == checksum_oldest_first
    finally:
        connection.rollback()
        connection.close()


def test_runtime_reconciliation_detects_missing_and_outdated_mart_winners():
    connection = get_connection()
    product_code = "7" * 60
    older_source = "test_reconciliation_older.json.gz"
    newer_source = "test_reconciliation_newer.json.gz"
    invalid_source = "test_reconciliation_invalid.json.gz"
    try:
        for source_file in (older_source, newer_source, invalid_source):
            connection.execute(
                "INSERT INTO raw.source_files (source_file_name) VALUES (%s)",
                (source_file,),
            )
        raw_rows = [
            (
                older_source,
                1,
                {"code": product_code, "last_modified_t": 1_900_000_050},
            ),
            (
                newer_source,
                1,
                {"code": product_code, "last_modified_t": 1_900_000_150},
            ),
            (
                invalid_source,
                1,
                {"code": product_code, "last_modified_t": "not-an-integer"},
            ),
        ]
        for source_file, line_number, payload in raw_rows:
            connection.execute(
                """
                INSERT INTO raw.product_records (
                    source_file_name, source_line_number, payload
                ) VALUES (%s, %s, %s::jsonb)
                """,
                (source_file, line_number, json.dumps(payload)),
            )

        assert stage_pending_records(connection) == 3
        refresh_mart(connection)
        assert connection.execute(
            """
            SELECT source_file_name
            FROM mart.products
            WHERE product_code = %s
            """,
            (product_code,),
        ).fetchone() == (newer_source,)
        assert connection.execute(
            """
            SELECT is_valid
            FROM staging.product_records
            WHERE source_file_name = %s
            """,
            (invalid_source,),
        ).fetchone() == (False,)

        connection.execute("SAVEPOINT missing_winner")
        connection.execute(
            "DELETE FROM mart.products WHERE product_code = %s",
            (product_code,),
        )
        with pytest.raises(DataQualityError, match="winner_difference_rows=1"):
            assert_layer_reconciliation(connection)
        connection.execute("ROLLBACK TO SAVEPOINT missing_winner")
        connection.execute("RELEASE SAVEPOINT missing_winner")

        connection.execute("SAVEPOINT outdated_winner")
        connection.execute(
            """
            UPDATE mart.products
            SET source_file_name = %s,
                source_line_number = 1,
                last_modified_at = to_timestamp(1900000050)
            WHERE product_code = %s
            """,
            (older_source, product_code),
        )
        with pytest.raises(DataQualityError, match="winner_difference_rows=2"):
            assert_layer_reconciliation(connection)
        connection.execute("ROLLBACK TO SAVEPOINT outdated_winner")
        connection.execute("RELEASE SAVEPOINT outdated_winner")

        with pytest.raises(CheckViolation):
            with connection.transaction():
                connection.execute(
                    """
                    INSERT INTO mart.products (
                        product_code,
                        last_modified_at,
                        nutriscore_status,
                        source_file_name,
                        source_line_number
                    ) VALUES ('not-digits', now(), 'missing', %s, 1)
                    """,
                    (older_source,),
                )
    finally:
        connection.rollback()
        connection.close()


def test_full_rebuild_recomputes_existing_staging_rows():
    connection = get_connection()
    source_file = "test_products_rebuild.json.gz"
    product_code = "8" * 60
    try:
        connection.execute(
            "INSERT INTO raw.source_files (source_file_name) VALUES (%s)",
            (source_file,),
        )
        original_payload = {
            "code": product_code,
            "last_modified_t": 1_789_689_600,
            "product_name": "Before rebuild",
        }
        connection.execute(
            """
            INSERT INTO raw.product_records (
                source_file_name, source_line_number, payload
            ) VALUES (%s, 1, %s::jsonb)
            """,
            (source_file, json.dumps(original_payload)),
        )
        assert stage_pending_records(connection) == 1
        refresh_mart(connection)

        changed_payload = {**original_payload, "product_name": "After rebuild"}
        connection.execute(
            """
            UPDATE raw.product_records
            SET payload = %s::jsonb
            WHERE source_file_name = %s AND source_line_number = 1
            """,
            (json.dumps(changed_payload), source_file),
        )

        rebuilt_staging_rows, rebuilt_mart_rows = rebuild_staging_and_mart(connection)

        assert rebuilt_staging_rows == connection.execute(
            "SELECT count(*) FROM raw.product_records"
        ).fetchone()[0]
        assert rebuilt_mart_rows == connection.execute(
            "SELECT count(*) FROM mart.products"
        ).fetchone()[0]
        assert connection.execute(
            """
            SELECT product_name
            FROM staging.product_records
            WHERE source_file_name = %s AND source_line_number = 1
            """,
            (source_file,),
        ).fetchone() == ("After rebuild",)
        assert connection.execute(
            """
            SELECT refresh_mode
            FROM mart.refresh_log
            ORDER BY refresh_id DESC
            LIMIT 1
            """
        ).fetchone() == (STAGING_AND_MART_REBUILD,)
    finally:
        connection.rollback()
        connection.close()


def test_failed_full_rebuild_rolls_back_staging_and_mart(monkeypatch):
    connection = get_connection()
    try:
        staging_before = table_checksum(connection, "staging")
        mart_before = table_checksum(connection, "mart")

        def fail_quality(_connection):
            raise DataQualityError("synthetic rebuild failure")

        monkeypatch.setattr("src.transform.assert_staging_quality", fail_quality)

        with pytest.raises(DataQualityError, match="synthetic rebuild failure"):
            with connection.transaction():
                rebuild_staging_and_mart(connection)

        assert table_checksum(connection, "staging") == staging_before
        assert table_checksum(connection, "mart") == mart_before
    finally:
        connection.rollback()
        connection.close()


def _insert_raw_batch(connection, source_file, payloads):
    connection.execute(
        "INSERT INTO raw.source_files (source_file_name) VALUES (%s)",
        (source_file,),
    )
    for line_number, payload in enumerate(payloads, start=1):
        connection.execute(
            """
            INSERT INTO raw.product_records (
                source_file_name, source_line_number, payload
            ) VALUES (%s, %s, %s::jsonb)
            """,
            (source_file, line_number, json.dumps(payload)),
        )


def _latest_refresh(connection):
    return connection.execute(
        """
        SELECT refresh_mode, affected_product_count
        FROM mart.refresh_log
        ORDER BY refresh_id DESC
        LIMIT 1
        """
    ).fetchone()


def _mart_winner(connection, product_code):
    return connection.execute(
        "SELECT product_name FROM mart.products WHERE product_code = %s",
        (product_code,),
    ).fetchone()


CODE_A, CODE_B, CODE_C = "81" * 30, "82" * 30, "83" * 30


def test_incremental_refresh_matches_full_refresh_across_out_of_order_batches():
    connection = get_connection()
    try:
        refresh_mart(connection)

        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900001000_1900002000.json.gz",
            [
                {"code": CODE_A, "last_modified_t": 1_900_001_500, "product_name": "A2"},
                {"code": CODE_B, "last_modified_t": 1_900_001_600, "product_name": "B1"},
            ],
        )
        stage_pending_records(connection)
        refresh_mart(connection)
        assert _latest_refresh(connection) == (MART_INCREMENTAL, 2)

        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900000000_1900001000.json.gz",
            [
                {"code": CODE_A, "last_modified_t": 1_900_000_500, "product_name": "A1"},
                {"code": CODE_C, "last_modified_t": 1_900_000_600, "product_name": "C1"},
            ],
        )
        stage_pending_records(connection)
        refresh_mart(connection)
        assert _latest_refresh(connection) == (MART_INCREMENTAL, 2)

        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900002000_1900003000.json.gz",
            [{"code": CODE_B, "last_modified_t": 1_900_002_500, "product_name": "B2"}],
        )
        stage_pending_records(connection)
        refresh_mart(connection)
        assert _latest_refresh(connection) == (MART_INCREMENTAL, 1)

        assert _mart_winner(connection, CODE_A) == ("A2",)
        assert _mart_winner(connection, CODE_B) == ("B2",)
        assert _mart_winner(connection, CODE_C) == ("C1",)
        incremental_checksum = table_checksum(connection, "mart")

        refresh_mart(connection, refresh_mode=MART_FULL_REFRESH)
        assert table_checksum(connection, "mart") == incremental_checksum
    finally:
        connection.rollback()
        connection.close()


def test_failed_incremental_refresh_is_applied_by_the_next_refresh(monkeypatch):
    connection = get_connection()
    try:
        refresh_mart(connection)
        checksum_before = table_checksum(connection, "mart")
        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900003000_1900004000.json.gz",
            [{"code": CODE_A, "last_modified_t": 1_900_003_500, "product_name": "A"}],
        )
        stage_pending_records(connection)

        def fail_reconciliation(_connection, *, affected_only):
            raise DataQualityError("synthetic refresh failure")

        with monkeypatch.context() as patch:
            patch.setattr(
                "src.transform.assert_layer_reconciliation", fail_reconciliation
            )
            with pytest.raises(DataQualityError, match="synthetic refresh failure"):
                with connection.transaction():
                    refresh_mart(connection)

        assert table_checksum(connection, "mart") == checksum_before
        assert connection.execute(
            """
            SELECT count(*)
            FROM staging.product_records
            WHERE product_code = %s AND NOT mart_applied
            """,
            (CODE_A,),
        ).fetchone() == (1,)

        refresh_mart(connection)
        assert _mart_winner(connection, CODE_A) == ("A",)
        assert _latest_refresh(connection) == (MART_INCREMENTAL, 1)
        recovered_checksum = table_checksum(connection, "mart")
        refresh_mart(connection, refresh_mode=MART_FULL_REFRESH)
        assert table_checksum(connection, "mart") == recovered_checksum
    finally:
        connection.rollback()
        connection.close()


def test_incremental_refresh_rewrites_only_affected_codes_and_noop_is_stable():
    connection = get_connection()
    try:
        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900004000_1900005000.json.gz",
            [
                {"code": CODE_A, "last_modified_t": 1_900_004_100, "product_name": "A1"},
                {"code": CODE_B, "last_modified_t": 1_900_004_200, "product_name": "B1"},
            ],
        )
        stage_pending_records(connection)
        refresh_mart(connection)

        def physical_row(product_code):
            return connection.execute(
                "SELECT ctid::text FROM mart.products WHERE product_code = %s",
                (product_code,),
            ).fetchone()

        untouched_before = physical_row(CODE_B)
        rewritten_before = physical_row(CODE_A)

        _insert_raw_batch(
            connection,
            "openfoodfacts_products_1900005000_1900006000.json.gz",
            [{"code": CODE_A, "last_modified_t": 1_900_005_100, "product_name": "A2"}],
        )
        stage_pending_records(connection)
        refresh_mart(connection)

        assert _latest_refresh(connection) == (MART_INCREMENTAL, 1)
        assert physical_row(CODE_B) == untouched_before
        assert physical_row(CODE_A) != rewritten_before
        assert _mart_winner(connection, CODE_A) == ("A2",)

        checksum_before_noop = table_checksum(connection, "mart")
        refresh_mart(connection)
        assert _latest_refresh(connection) == (MART_INCREMENTAL, 0)
        assert table_checksum(connection, "mart") == checksum_before_noop
    finally:
        connection.rollback()
        connection.close()


def test_refresh_fails_fast_when_a_conflicting_lock_is_held(monkeypatch):
    blocker = get_connection()
    connection = get_connection()
    try:
        blocker.execute("LOCK TABLE staging.product_records IN ROW EXCLUSIVE MODE")
        monkeypatch.setattr("src.transform.LOCK_TIMEOUT", "200ms")

        with pytest.raises(LockNotAvailable):
            refresh_mart(connection)
    finally:
        connection.rollback()
        connection.close()
        blocker.rollback()
        blocker.close()


def test_mart_insert_columns_match_the_mart_table():
    with get_connection() as connection:
        table_columns = [
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'mart' AND table_name = 'products'
                ORDER BY ordinal_position
                """
            ).fetchall()
        ]
    insert_block = MART_INSERT_SQL.split("INSERT INTO mart.products (", 1)[1]
    insert_columns = [c.strip() for c in insert_block.split(")", 1)[0].split(",")]

    assert insert_columns == table_columns


def test_gap_check_reads_only_loaded_source_files():
    loaded = "openfoodfacts_products_1900009000_1900010000.json.gz"
    pending = "openfoodfacts_products_1900010000_1900011000.json.gz"
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO raw.source_files (
                source_file_name, load_status, load_started_at,
                load_completed_at, loaded_row_count
            ) VALUES
                (%s, 'loaded', now(), now(), 0),
                (%s, 'pending', NULL, NULL, NULL)
            """,
            (loaded, pending),
        )
    try:
        names = _loaded_source_file_names()
        assert loaded in names
        assert pending not in names
    finally:
        with get_connection() as connection:
            connection.execute(
                "DELETE FROM raw.source_files WHERE source_file_name IN (%s, %s)",
                (loaded, pending),
            )
