"""Load local Open Food Facts JSON Lines gzip files into the raw layer."""

from __future__ import annotations

import argparse
import gzip
import json
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from psycopg import Connection, DataError
from psycopg.types.json import Jsonb

from src.db import get_connection, load_sql
from src.source_files import parse_source_file_name


LOAD_BATCH_SIZE = 100
T = TypeVar("T")


class FileLoadError(ValueError):
    """Raised when a source line cannot be loaded as a JSON object."""


class LoadStateError(RuntimeError):
    """Raised when raw rows contradict the source-file load state."""


class _NonStandardJsonConstant(ValueError):
    """Raised for JSON constants that PostgreSQL JSONB does not accept."""


@dataclass(frozen=True)
class LoadResult:
    """Outcome of loading one local source file."""

    source_file_name: str
    loaded_row_count: int
    skipped: bool


def _validated_path(file_path: str | Path) -> Path:
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"source file does not exist: {path}")
    if not path.name.endswith(".json.gz"):
        raise ValueError(f"source file must end with .json.gz: {path}")
    return path


def _expand_paths(file_paths: Iterable[str | Path]) -> Iterator[Path]:
    for file_path in file_paths:
        path = Path(file_path)
        if path.is_dir():
            files = sorted(path.glob("*.json.gz"), key=_file_sort_key)
            if not files:
                raise FileNotFoundError(f"no .json.gz files found in directory: {path}")
            yield from files
        else:
            yield _validated_path(path)


def _file_sort_key(path: Path) -> tuple[int, int, int, str]:
    try:
        start, end = parse_source_file_name(path.name)
    except ValueError:
        return (1, 0, 0, path.name)
    return (0, start, end, path.name)


def _reject_nonstandard_json_constant(value: str) -> None:
    raise _NonStandardJsonConstant(value)


def _iter_payloads(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    with gzip.open(path, mode="rb") as source:
        line_number = 0
        while True:
            try:
                raw_line = source.readline()
            except (gzip.BadGzipFile, EOFError, OSError, zlib.error) as error:
                raise FileLoadError(
                    f"cannot load {path.name}: gzip read failed near line "
                    f"{line_number + 1} ({error})"
                ) from error
            if not raw_line:
                return
            line_number += 1
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError as error:
                raise FileLoadError(
                    f"cannot load {path.name}: line {line_number} is not valid UTF-8"
                ) from error
            try:
                payload = json.loads(
                    line,
                    parse_constant=_reject_nonstandard_json_constant,
                )
            except _NonStandardJsonConstant as error:
                raise FileLoadError(
                    f"cannot load {path.name}: line {line_number} contains "
                    f"non-standard JSON constant {error.args[0]}"
                ) from error
            except json.JSONDecodeError as error:
                raise FileLoadError(
                    f"cannot load {path.name}: line {line_number} is not valid JSON "
                    f"({error.msg})"
                ) from error
            if not isinstance(payload, dict):
                raise FileLoadError(
                    f"cannot load {path.name}: line {line_number} must be a JSON object, "
                    f"got {type(payload).__name__}"
                )
            yield line_number, payload


REGISTER_SOURCE_FILE_SQL = load_sql("load/register_source_file.sql")
LOCK_SOURCE_FILE_SQL = load_sql("load/lock_source_file.sql")
COUNT_FILE_RAW_ROWS_SQL = load_sql("load/count_raw_rows_for_file.sql")
MARK_LOAD_STARTED_SQL = load_sql("load/mark_load_started.sql")
RAW_INSERT_SQL = load_sql("load/insert_raw_record.sql")
MARK_FILE_LOADED_SQL = load_sql("load/mark_file_loaded.sql")


def _insert_batch(connection: Connection, batch: list[tuple[object, ...]]) -> None:
    """Insert a batch, identifying an individual row after a data error."""
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.executemany(RAW_INSERT_SQL, batch)
    except DataError as batch_error:
        for row in batch:
            try:
                with connection.transaction(force_rollback=True):
                    connection.execute(RAW_INSERT_SQL, row)
            except DataError as row_error:
                message = row_error.diag.message_primary or str(row_error).splitlines()[0]
                raise FileLoadError(
                    f"cannot load {row[0]}: line {row[1]} was rejected by "
                    f"PostgreSQL ({message})"
                ) from row_error
        raise FileLoadError(
            f"cannot load {batch[0][0]}: PostgreSQL rejected lines "
            f"{batch[0][1]}-{batch[-1][1]} ({batch_error})"
        ) from batch_error


def _batches(values: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for value in values:
        batch.append(value)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def register_source_file(connection: Connection, file_path: str | Path) -> str:
    """Register a local file as pending without changing an existing status."""
    path = _validated_path(file_path)
    source_file_name = path.name
    connection.execute(REGISTER_SOURCE_FILE_SQL, (source_file_name,))
    return source_file_name


def load_file(connection: Connection, file_path: str | Path) -> LoadResult:
    """Load one file in the caller's transaction, or skip it if already loaded.

    This function deliberately does not commit or roll back. The caller owns the
    transaction boundary so tests and orchestrators can control atomicity.
    """
    path = _validated_path(file_path)
    source_file_name = path.name

    connection.execute(REGISTER_SOURCE_FILE_SQL, (source_file_name,))
    status_row = connection.execute(
        LOCK_SOURCE_FILE_SQL, (source_file_name,)
    ).fetchone()
    if status_row is None:
        raise LoadStateError(f"source-file registry row disappeared: {source_file_name}")
    if status_row[0] == "loaded":
        return LoadResult(source_file_name, status_row[1], skipped=True)

    existing_row_count = connection.execute(
        COUNT_FILE_RAW_ROWS_SQL, (source_file_name,)
    ).fetchone()[0]
    if existing_row_count:
        raise LoadStateError(
            f"cannot load {source_file_name}: expected 0 existing raw rows for a "
            f"pending file, found {existing_row_count}"
        )

    connection.execute(MARK_LOAD_STARTED_SQL, (source_file_name,))

    rows = (
        (source_file_name, line_number, Jsonb(payload))
        for line_number, payload in _iter_payloads(path)
    )
    loaded_row_count = 0
    for batch in _batches(rows, LOAD_BATCH_SIZE):
        _insert_batch(connection, batch)
        loaded_row_count += len(batch)

    connection.execute(MARK_FILE_LOADED_SQL, (loaded_row_count, source_file_name))
    return LoadResult(source_file_name, loaded_row_count, skipped=False)


def run_load(file_paths: Iterable[str | Path]) -> list[LoadResult]:
    """Register and atomically load each local file."""
    results = []
    for file_path in _expand_paths(file_paths):
        with get_connection() as connection:
            register_source_file(connection, file_path)
        with get_connection() as connection:
            results.append(load_file(connection, file_path))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files",
        nargs="+",
        help="local .json.gz files or directories containing them",
    )
    args = parser.parse_args()

    for result in run_load(args.files):
        action = "skipped" if result.skipped else "loaded"
        print(
            f"{result.source_file_name}: {action} "
            f"({result.loaded_row_count:,} rows)"
        )


if __name__ == "__main__":
    main()
