"""PostgreSQL connection helper."""

from __future__ import annotations

import functools
import os
from pathlib import Path

import psycopg


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/openfoodfacts"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def get_connection() -> psycopg.Connection:
    """Return a connection using DATABASE_URL or the starter default."""
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    return psycopg.connect(database_url)


@functools.cache
def load_sql(relative_path: str) -> str:
    """Return one SQL statement from the repository's ``sql/`` directory."""
    return (SQL_DIR / relative_path).read_text(encoding="utf-8")
