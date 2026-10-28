"""Test-session guards: no network access and a disposable PostgreSQL database.

Integration tests run automatically when PostgreSQL is reachable, against a
dedicated ``<database>_test`` database that is created and dropped here, so a
test run never reads or writes the pipeline's own database. Set
``RUN_DB_TESTS=0`` to skip them explicitly.
"""

from __future__ import annotations

import os
import socket

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.sql import SQL, Identifier

from src.db import DEFAULT_DATABASE_URL
from src.schema import apply_schema


_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_original_connect = socket.socket.connect
_original_getaddrinfo = socket.getaddrinfo
_TEST_DATABASE: dict[str, str] = {}


def _is_local(host: object) -> bool:
    # Unix-domain socket paths are local as well.
    return host is None or (
        isinstance(host, str) and (host in _LOCAL_HOSTS or host.startswith("/"))
    )


def _guarded_connect(self, address):
    host = address[0] if isinstance(address, tuple) else address
    if not _is_local(host):
        raise RuntimeError(f"tests must not use the network: connect to {address!r}")
    return _original_connect(self, address)


def _guarded_getaddrinfo(host, *args, **kwargs):
    if not _is_local(host):
        raise RuntimeError(f"tests must not use the network: resolve {host!r}")
    return _original_getaddrinfo(host, *args, **kwargs)


def _database_unavailable_reason() -> str | None:
    """Create and migrate the test database; return why that was impossible."""
    if os.getenv("RUN_DB_TESTS") == "0":
        return "RUN_DB_TESTS=0"

    source = conninfo_to_dict(os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    test_name = f"{source.get('dbname', 'openfoodfacts')}_test"
    admin_url = make_conninfo(**{**source, "dbname": "postgres", "connect_timeout": 3})
    try:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(SQL("DROP DATABASE IF EXISTS {}").format(Identifier(test_name)))
            admin.execute(SQL("CREATE DATABASE {}").format(Identifier(test_name)))
    except psycopg.Error as error:
        first_line = str(error).strip().splitlines()[0]
        return f"PostgreSQL unavailable ({first_line}); start it with docker compose"

    # src.db reads DATABASE_URL on every connection, so the whole session,
    # including code under test, now targets the disposable database.
    os.environ["DATABASE_URL"] = make_conninfo(**{**source, "dbname": test_name})
    apply_schema()
    apply_schema()  # the DAG bootstraps the schema on every run
    _TEST_DATABASE["admin_url"] = admin_url
    _TEST_DATABASE["name"] = test_name
    return None


def pytest_configure(config):
    socket.socket.connect = _guarded_connect
    socket.getaddrinfo = _guarded_getaddrinfo
    config.database_unavailable_reason = _database_unavailable_reason()


def pytest_collection_modifyitems(config, items):
    reason = config.database_unavailable_reason
    if reason is None:
        return
    skip = pytest.mark.skip(reason=f"integration test skipped: {reason}")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


def pytest_unconfigure(config):
    socket.socket.connect = _original_connect
    socket.getaddrinfo = _original_getaddrinfo
    if _TEST_DATABASE:
        with psycopg.connect(_TEST_DATABASE["admin_url"], autocommit=True) as admin:
            admin.execute(
                SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    Identifier(_TEST_DATABASE["name"])
                )
            )
