from __future__ import annotations

import gzip
import json
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

import pytest

from src.db import get_connection
from src.load import (
    LOAD_BATCH_SIZE,
    FileLoadError,
    LoadStateError,
    _batches,
    _expand_paths,
    _iter_payloads,
    load_file,
    register_source_file,
    run_load,
)
from src.verification import checksum_rows


def _write_gzip(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8") as target:
        target.writelines(lines)
    return path


def _write_midstream_corrupt_gzip(path: Path) -> Path:
    first_raw = b"".join(
        f'{{"code":"{index:06d}","noise":"{index * index:012d}"}}\n'.encode()
        for index in range(500)
    )
    second_raw = b"".join(
        f'{{"code":"{index:06d}","noise":"{index * index:012d}"}}\n'.encode()
        for index in range(500, 1_000)
    )
    first_member = gzip.compress(first_raw)
    compressed = bytearray(first_member + gzip.compress(second_raw))
    corrupt_at = len(first_member) + 10
    compressed[corrupt_at : corrupt_at + 64] = bytes(
        byte ^ 0xFF for byte in compressed[corrupt_at : corrupt_at + 64]
    )
    with pytest.raises(zlib.error):
        gzip.decompress(compressed)
    path.write_bytes(compressed)
    return path


def test_iter_payloads_preserves_physical_line_numbers_and_values(tmp_path):
    path = _write_gzip(
        tmp_path / "delta.json.gz",
        [
            json.dumps({"code": "00123"}) + "\n",
            json.dumps({"code": "00456"}) + "\n",
        ],
    )

    assert list(_iter_payloads(path)) == [
        (1, {"code": "00123"}),
        (2, {"code": "00456"}),
    ]


def test_blank_line_fails_with_physical_line_number(tmp_path):
    path = _write_gzip(
        tmp_path / "delta.json.gz",
        [json.dumps({"code": "00123"}) + "\n", "\n"],
    )

    with pytest.raises(FileLoadError, match=r"delta\.json\.gz: line 2"):
        list(_iter_payloads(path))


@pytest.mark.parametrize("value", [[], "text", 1, None])
def test_non_object_json_fails(value, tmp_path):
    path = _write_gzip(tmp_path / "delta.json.gz", [json.dumps(value) + "\n"])

    with pytest.raises(FileLoadError, match=r"line 1 must be a JSON object"):
        list(_iter_payloads(path))


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_non_standard_json_constants_fail_with_line_number(constant, tmp_path):
    path = _write_gzip(
        tmp_path / "delta.json.gz",
        [f'{{"code":"00123","value":{constant}}}\n'],
    )

    with pytest.raises(
        FileLoadError,
        match=rf"delta\.json\.gz: line 1.*{constant}",
    ):
        list(_iter_payloads(path))


def test_invalid_utf8_fails_with_physical_line_number(tmp_path):
    path = tmp_path / "delta.json.gz"
    with gzip.open(path, mode="wb") as target:
        target.write(b'{"code":"00123"}\n')
        target.write(b'{"code":"\xff"}\n')

    with pytest.raises(FileLoadError, match=r"delta\.json\.gz: line 2.*UTF-8"):
        list(_iter_payloads(path))


def test_midstream_zlib_corruption_is_wrapped_as_file_load_error(tmp_path):
    path = _write_midstream_corrupt_gzip(tmp_path / "delta.json.gz")

    with pytest.raises(
        FileLoadError,
        match=r"cannot load delta\.json\.gz: gzip read failed near line \d+",
    ):
        list(_iter_payloads(path))


def test_batches_use_configured_size():
    batches = list(_batches(range(LOAD_BATCH_SIZE * 2 + 1), LOAD_BATCH_SIZE))

    assert [len(batch) for batch in batches] == [100, 100, 1]


def test_expand_paths_sorts_gzip_files_in_directory(tmp_path):
    later = _write_gzip(
        tmp_path / "openfoodfacts_products_10_11.json.gz",
        [],
    )
    earlier = _write_gzip(
        tmp_path / "openfoodfacts_products_9_10.json.gz",
        [],
    )
    fallback_b = _write_gzip(tmp_path / "b.json.gz", [])
    fallback_a = _write_gzip(tmp_path / "a.json.gz", [])
    (tmp_path / "index.txt").write_text("ignored", encoding="utf-8")

    assert list(_expand_paths([tmp_path])) == [
        earlier,
        later,
        fallback_a,
        fallback_b,
    ]


@pytest.mark.integration
def test_load_file_uses_basename_and_rerun_preserves_raw_checksum(tmp_path):
    first_path = _write_gzip(
        tmp_path / "data" / "test_loader_idempotent.json.gz",
        [
            json.dumps({"code": "00123", "last_modified_t": 1_789_000_000})
            + "\n",
            json.dumps({"code": "00456", "last_modified_t": 1_789_000_001})
            + "\n",
        ],
    )
    second_path = _write_gzip(
        tmp_path / "sample_data" / first_path.name,
        [json.dumps({"code": "this-must-not-replace-raw"}) + "\n"],
    )
    connection = get_connection()
    try:
        register_source_file(connection, first_path)
        first_result = load_file(connection, first_path)

        assert first_result.source_file_name == first_path.name
        assert first_result.loaded_row_count == 2
        assert first_result.skipped is False
        assert connection.execute(
            """
            SELECT load_status, loaded_row_count
            FROM raw.source_files
            WHERE source_file_name = %s
            """,
            (first_path.name,),
        ).fetchone() == ("loaded", 2)

        sentinel = datetime(2000, 1, 1, tzinfo=UTC)
        connection.execute(
            """
            UPDATE raw.product_records
            SET loaded_at = %s
            WHERE source_file_name = %s
            """,
            (sentinel, first_path.name),
        )
        checksum_query = """
            SELECT source_file_name, source_line_number, payload, loaded_at
            FROM raw.product_records
            WHERE source_file_name = %s
            ORDER BY source_line_number
        """
        checksum_before = checksum_rows(
            connection.execute(checksum_query, (first_path.name,))
        )

        second_result = load_file(connection, second_path)
        checksum_after = checksum_rows(
            connection.execute(checksum_query, (first_path.name,))
        )

        assert second_result.loaded_row_count == 2
        assert second_result.skipped is True
        assert checksum_after == checksum_before
        assert connection.execute(
            """
            SELECT payload->>'code', loaded_at
            FROM raw.product_records
            WHERE source_file_name = %s
            ORDER BY source_line_number
            """,
            (first_path.name,),
        ).fetchall() == [("00123", sentinel), ("00456", sentinel)]
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.integration
def test_malformed_json_rolls_back_file_and_leaves_pending(tmp_path):
    path = _write_gzip(
        tmp_path / "test_loader_malformed.json.gz",
        [json.dumps({"code": "00123"}) + "\n", "not-json\n"],
    )
    connection = get_connection()
    try:
        register_source_file(connection, path)
        with pytest.raises(FileLoadError, match=r"line 2"):
            with connection.transaction():
                load_file(connection, path)

        assert connection.execute(
            """
            SELECT load_status
            FROM raw.source_files
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == ("pending",)
        assert connection.execute(
            """
            SELECT count(*)
            FROM raw.product_records
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == (0,)
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.integration
def test_pending_file_with_residual_rows_fails_with_context(tmp_path):
    path = _write_gzip(
        tmp_path / "test_loader_residual.json.gz",
        [json.dumps({"code": "00123"}) + "\n"],
    )
    connection = get_connection()
    try:
        register_source_file(connection, path)
        connection.execute(
            """
            INSERT INTO raw.product_records (
                source_file_name, source_line_number, payload
            ) VALUES (%s, 1, %s::jsonb)
            """,
            (path.name, json.dumps({"code": "residual"})),
        )

        with pytest.raises(
            LoadStateError,
            match=rf"{path.name}.*expected 0.*found 1",
        ):
            load_file(connection, path)
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.integration
def test_empty_file_loads_as_zero_row_completed_delta(tmp_path):
    path = _write_gzip(tmp_path / "test_loader_empty.json.gz", [])
    connection = get_connection()
    try:
        result = load_file(connection, path)

        assert result.loaded_row_count == 0
        assert result.skipped is False
        assert connection.execute(
            """
            SELECT load_status, loaded_row_count
            FROM raw.source_files
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == ("loaded", 0)
        assert connection.execute(
            """
            SELECT count(*)
            FROM raw.product_records
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == (0,)
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.integration
def test_postgres_jsonb_error_is_wrapped_with_file_and_line(tmp_path):
    path = _write_gzip(
        tmp_path / "test_loader_null_character.json.gz",
        [
            '{"code":"00123"}\n',
            '{"code":"00456","bad":"\\u0000"}\n',
        ],
    )
    connection = get_connection()
    try:
        register_source_file(connection, path)
        with pytest.raises(
            FileLoadError,
            match=rf"{path.name}: line 2.*PostgreSQL",
        ):
            with connection.transaction():
                load_file(connection, path)

        assert connection.execute(
            """
            SELECT load_status
            FROM raw.source_files
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == ("pending",)
        assert connection.execute(
            """
            SELECT count(*)
            FROM raw.product_records
            WHERE source_file_name = %s
            """,
            (path.name,),
        ).fetchone() == (0,)
    finally:
        connection.rollback()
        connection.close()


def _delete_committed_source_files(*source_file_names: str) -> None:
    with get_connection() as connection:
        connection.execute(
            "DELETE FROM raw.product_records WHERE source_file_name = ANY(%s)",
            (list(source_file_names),),
        )
        connection.execute(
            "DELETE FROM raw.source_files WHERE source_file_name = ANY(%s)",
            (list(source_file_names),),
        )


@pytest.mark.integration
def test_concurrent_loads_commit_one_copy_and_skip_the_other(tmp_path):
    path = _write_gzip(
        tmp_path / "test_loader_concurrent.json.gz",
        [json.dumps({"code": "00123"}) + "\n"],
    )
    _delete_committed_source_files(path.name)
    barrier = Barrier(2)

    def load_concurrently():
        with get_connection() as connection:
            barrier.wait()
            return load_file(connection, path)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: load_concurrently(), range(2)))

        assert sorted(result.skipped for result in results) == [False, True]
        with get_connection() as connection:
            assert connection.execute(
                """
                SELECT load_status, loaded_row_count
                FROM raw.source_files
                WHERE source_file_name = %s
                """,
                (path.name,),
            ).fetchone() == ("loaded", 1)
            assert connection.execute(
                """
                SELECT count(*)
                FROM raw.product_records
                WHERE source_file_name = %s
                """,
                (path.name,),
            ).fetchone() == (1,)
    finally:
        _delete_committed_source_files(path.name)


@pytest.mark.integration
def test_bad_middle_file_stops_strictly_ordered_batch(tmp_path):
    first = _write_gzip(
        tmp_path / "test_loader_sequence_a.json.gz",
        [json.dumps({"code": "00123"}) + "\n"],
    )
    bad = _write_gzip(
        tmp_path / "test_loader_sequence_b.json.gz",
        ['{"code":"00456","value":NaN}\n'],
    )
    third = _write_gzip(
        tmp_path / "test_loader_sequence_c.json.gz",
        [json.dumps({"code": "00789"}) + "\n"],
    )
    names = (first.name, bad.name, third.name)
    _delete_committed_source_files(*names)

    try:
        with pytest.raises(FileLoadError, match=rf"{bad.name}: line 1.*NaN"):
            run_load([first, bad, third])

        with get_connection() as connection:
            assert connection.execute(
                """
                SELECT source_file_name, load_status
                FROM raw.source_files
                WHERE source_file_name = ANY(%s)
                ORDER BY source_file_name
                """,
                (list(names),),
            ).fetchall() == [
                (first.name, "loaded"),
                (bad.name, "pending"),
            ]
            assert connection.execute(
                """
                SELECT source_file_name, count(*)
                FROM raw.product_records
                WHERE source_file_name = ANY(%s)
                GROUP BY source_file_name
                ORDER BY source_file_name
                """,
                (list(names),),
            ).fetchall() == [(first.name, 1)]
    finally:
        _delete_committed_source_files(*names)
