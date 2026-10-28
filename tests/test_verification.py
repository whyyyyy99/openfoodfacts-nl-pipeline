from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.verification import CHECKSUM_QUERIES, checksum_rows, table_checksum


def test_checksum_is_stable_for_equivalent_json_object_order():
    first = checksum_rows([({"b": 2, "a": 1}, Decimal("1.20"))])
    second = checksum_rows([({"a": 1, "b": 2}, Decimal("1.20"))])

    assert first == second


def test_checksum_changes_when_provenance_changes():
    loaded_at = datetime(2026, 9, 18, tzinfo=UTC)
    first = checksum_rows([("file-a", 1, {"code": "1"}, loaded_at)])
    second = checksum_rows([("file-a", 2, {"code": "1"}, loaded_at)])

    assert first != second


def test_only_raw_checksum_includes_loaded_at():
    assert "loaded_at" in CHECKSUM_QUERIES["raw"]
    assert "loaded_at" not in CHECKSUM_QUERIES["staging"]
    assert "loaded_at" not in CHECKSUM_QUERIES["mart"]


def test_downstream_checksums_include_nutriscore_status():
    assert "nutriscore_status" in CHECKSUM_QUERIES["staging"]
    assert "nutriscore_status" in CHECKSUM_QUERIES["mart"]


def test_unknown_checksum_layer_is_rejected():
    with pytest.raises(ValueError, match="unknown checksum layer"):
        table_checksum(None, "unknown")  # type: ignore[arg-type]


class _StreamingCursor:
    def __init__(self, rows):
        self.rows = rows
        self.itersize = None
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query):
        self.query = query
        return self

    def __iter__(self):
        return iter(self.rows)


class _StreamingConnection:
    def __init__(self, rows):
        self.server_cursor = _StreamingCursor(rows)
        self.cursor_name = None

    def cursor(self, *, name):
        self.cursor_name = name
        return self.server_cursor


def test_table_checksum_uses_named_streaming_cursor():
    connection = _StreamingConnection([("00123",)])

    result = table_checksum(connection, "mart")  # type: ignore[arg-type]

    assert result == checksum_rows([("00123",)])
    assert connection.cursor_name == "mart_checksum_stream"
    assert connection.server_cursor.itersize == 100
