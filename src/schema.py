"""Apply the PostgreSQL schema definition."""

from __future__ import annotations

from pathlib import Path

from src.db import get_connection


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


def apply_schema() -> None:
    """Create the project schemas, tables, constraints, and indexes."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_connection() as connection:
        connection.execute(schema_sql)


if __name__ == "__main__":
    apply_schema()
