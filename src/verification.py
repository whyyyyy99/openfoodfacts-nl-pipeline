"""Deterministic content checksums for rerun verification."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg import Connection

from src.db import load_sql


STREAM_BATCH_SIZE = 100

CHECKSUM_QUERIES = {
    layer: load_sql(f"verification/checksum_{layer}.sql")
    for layer in ("raw", "staging", "mart")
}


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def checksum_rows(rows: Iterable[Sequence[Any]]) -> str:
    """Hash rows in caller-supplied canonical order."""
    digest = hashlib.sha256()
    for row in rows:
        encoded = json.dumps(
            _json_value(row), ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def table_checksum(connection: Connection, layer: str) -> str:
    """Return a checksum for an allow-listed layer and explicit stable columns."""
    try:
        query = CHECKSUM_QUERIES[layer]
    except KeyError as error:
        raise ValueError(f"unknown checksum layer: {layer}") from error
    with connection.cursor(name=f"{layer}_checksum_stream") as cursor:
        cursor.itersize = STREAM_BATCH_SIZE
        cursor.execute(query)
        return checksum_rows(cursor)
