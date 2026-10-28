"""Run the Open Food Facts extract, load, transform, and verification pipeline."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests

from src.db import get_connection, load_sql
from src.extract import DEFAULT_BASE_URL, default_data_dir, ensure_files, fetch_index
from src.load import run_load
from src.schema import apply_schema
from src.transform import run_transform
from src.verification import table_checksum


@dataclass(frozen=True)
class PipelineResult:
    run_started_at: datetime
    selected_files: tuple[str, ...]
    loaded_source_files: tuple[str, ...]
    downloaded_file_count: int
    reused_file_count: int
    loaded_file_count: int
    skipped_file_count: int
    staged_row_count: int
    raw_row_count: int
    staging_row_count: int
    valid_staging_row_count: int
    invalid_staging_row_count: int
    non_winning_valid_row_count: int
    mart_row_count: int
    cross_file_product_count: int
    raw_checksum: str | None
    staging_checksum: str
    mart_checksum: str


@dataclass(frozen=True)
class DatabaseEvidence:
    loaded_source_files: tuple[str, ...]
    raw_row_count: int
    staging_row_count: int
    valid_staging_row_count: int
    invalid_staging_row_count: int
    non_winning_valid_row_count: int
    mart_row_count: int
    cross_file_product_count: int
    raw_checksum: str | None
    staging_checksum: str
    mart_checksum: str


EVIDENCE_SQL = load_sql("pipeline/evidence.sql")


def _database_evidence(*, verify: bool) -> DatabaseEvidence:
    with get_connection() as connection:
        counts = connection.execute(EVIDENCE_SQL).fetchone()
        raw_checksum = table_checksum(connection, "raw") if verify else None
        staging_checksum = table_checksum(connection, "staging")
        mart_checksum = table_checksum(connection, "mart")
    return DatabaseEvidence(
        loaded_source_files=tuple(counts[0]),
        raw_row_count=counts[1],
        staging_row_count=counts[2],
        valid_staging_row_count=counts[3],
        invalid_staging_row_count=counts[4],
        non_winning_valid_row_count=counts[5],
        mart_row_count=counts[6],
        cross_file_product_count=counts[7],
        raw_checksum=raw_checksum,
        staging_checksum=staging_checksum,
        mart_checksum=mart_checksum,
    )


def run_pipeline(
    *,
    data_dir: str | Path | None = None,
    count: int | None = None,
    file_names: list[str] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    verify: bool = False,
    rebuild: bool = False,
) -> PipelineResult:
    """Run all pipeline stages, stopping immediately if any stage fails."""
    if count is not None and count <= 0:
        raise ValueError("count must be greater than zero")
    if count is not None and file_names:
        raise ValueError("count cannot be combined with explicit filenames")

    run_started_at = datetime.now(UTC)
    destination_dir = Path(data_dir) if data_dir is not None else default_data_dir()
    apply_schema()
    with requests.Session() as session:
        if file_names:
            selected_names = file_names
        else:
            index_names = fetch_index(session, base_url)
            selected_names = (
                index_names[-count:] if count is not None else index_names
            )

        downloaded_names: set[str] = set()
        paths = ensure_files(
            session,
            destination_dir,
            selected_names,
            base_url=base_url,
            downloaded_names=downloaded_names,
        )

    load_results = run_load(paths)
    staged_row_count, _ = run_transform(rebuild=rebuild)
    evidence = _database_evidence(verify=verify)

    reused_file_count = len(paths) - len(downloaded_names)
    return PipelineResult(
        run_started_at=run_started_at,
        selected_files=tuple(path.name for path in paths),
        loaded_source_files=evidence.loaded_source_files,
        downloaded_file_count=len(downloaded_names),
        reused_file_count=reused_file_count,
        loaded_file_count=sum(not result.skipped for result in load_results),
        skipped_file_count=sum(result.skipped for result in load_results),
        staged_row_count=staged_row_count,
        raw_row_count=evidence.raw_row_count,
        staging_row_count=evidence.staging_row_count,
        valid_staging_row_count=evidence.valid_staging_row_count,
        invalid_staging_row_count=evidence.invalid_staging_row_count,
        non_winning_valid_row_count=evidence.non_winning_valid_row_count,
        mart_row_count=evidence.mart_row_count,
        cross_file_product_count=evidence.cross_file_product_count,
        raw_checksum=evidence.raw_checksum,
        staging_checksum=evidence.staging_checksum,
        mart_checksum=evidence.mart_checksum,
    )


def _positive_count(value: str) -> int:
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("--count must be greater than zero")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file_names", nargs="*", help="exact delta filenames")
    parser.add_argument("--count", type=_positive_count)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="delta directory (default: $OFF_DATA_DIR, else the project's data/)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="add the expensive raw checksum to the default staging and mart checksums",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="atomically rebuild staging and mart from all committed raw rows",
    )
    args = parser.parse_args()
    if args.count is not None and args.file_names:
        parser.error("--count cannot be combined with explicit filenames")

    result = run_pipeline(
        data_dir=args.data_dir,
        count=args.count,
        file_names=args.file_names or None,
        verify=args.verify,
        rebuild=args.rebuild,
    )
    print(f"run started at: {result.run_started_at.isoformat()}")
    print(f"selected files: {len(result.selected_files):,}")
    for file_name in result.selected_files:
        print(f"  selected: {file_name}")
    print(f"loaded source files: {len(result.loaded_source_files):,}")
    for file_name in result.loaded_source_files:
        print(f"  loaded: {file_name}")
    print(f"downloaded files: {result.downloaded_file_count:,}")
    print(f"reused local files: {result.reused_file_count:,}")
    print(f"loaded files: {result.loaded_file_count:,}")
    print(f"skipped files: {result.skipped_file_count:,}")
    print(f"staged this run: {result.staged_row_count:,}")
    print(f"raw rows: {result.raw_row_count:,}")
    print(f"staging rows: {result.staging_row_count:,}")
    print(f"valid staging rows: {result.valid_staging_row_count:,}")
    print(f"invalid staging rows: {result.invalid_staging_row_count:,}")
    print(f"non-winning valid rows: {result.non_winning_valid_row_count:,}")
    print(f"mart rows: {result.mart_row_count:,}")
    print(f"products in multiple loaded files: {result.cross_file_product_count:,}")
    if result.raw_checksum is not None:
        print(f"raw checksum: {result.raw_checksum}")
    print(f"staging checksum: {result.staging_checksum}")
    print(f"mart checksum: {result.mart_checksum}")


if __name__ == "__main__":
    main()
