"""Download Open Food Facts delta files from the live index."""

from __future__ import annotations

import argparse
import gzip
import os
import time
import zlib
from collections.abc import Iterable
from pathlib import Path

import requests

from src.source_files import parse_source_file_name


DEFAULT_BASE_URL = "https://static.openfoodfacts.org/data/delta"
REQUEST_TIMEOUT = (5, 60)
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 1
DOWNLOAD_CHUNK_SIZE = 64 * 1024
GZIP_PROBE_CHUNK_SIZE = 1024 * 1024
USER_AGENT = os.getenv(
    "OFF_USER_AGENT",
    "openfoodfacts-nl-pipeline/1.0 (https://github.com/whyyyyy99)",
)
RETRYABLE_NETWORK_ERRORS = (requests.Timeout, requests.ConnectionError)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR_ENV = "OFF_DATA_DIR"


class DownloadIntegrityError(IOError):
    """Raised when a downloaded file is not a complete, valid gzip stream."""


def default_data_dir() -> Path:
    """Return ``OFF_DATA_DIR``, else the project's ``data/`` directory.

    The fallback is anchored to this source file rather than the process
    working directory, which an Airflow worker does not guarantee.
    """
    configured = os.getenv(DATA_DIR_ENV)
    return Path(configured).expanduser() if configured else PROJECT_ROOT / "data"


def _sort_and_validate_names(names: Iterable[str]) -> list[str]:
    parsed = [(parse_source_file_name(name), name) for name in names]
    if len({name for _, name in parsed}) != len(parsed):
        raise ValueError("duplicate filename in delta index or selection")
    return [name for _, name in sorted(parsed, key=lambda item: (*item[0], item[1]))]


def _raise_for_non_2xx(response: requests.Response) -> None:
    if 200 <= response.status_code < 300:
        return
    response.raise_for_status()
    raise requests.HTTPError(
        f"unexpected HTTP status {response.status_code}", response=response
    )


def _sleep_before_retry(attempt: int) -> None:
    time.sleep(RETRY_DELAY_SECONDS * attempt)


def _set_user_agent(session: requests.Session) -> None:
    session.headers["User-Agent"] = USER_AGENT


def _validate_gzip(path: Path, file_name: str) -> None:
    try:
        with gzip.open(path, "rb") as probe:
            while probe.read(GZIP_PROBE_CHUNK_SIZE):
                pass
    except (gzip.BadGzipFile, EOFError, OSError, zlib.error) as error:
        raise DownloadIntegrityError(
            f"downloaded file is not a complete valid gzip: {file_name}"
        ) from error


def fetch_index(session: requests.Session, base_url: str) -> list[str]:
    """Fetch and validate the live index, returning filenames oldest first."""
    _set_user_agent(session)
    url = f"{base_url.rstrip('/')}/index.txt"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            with response:
                retry_server_error = (
                    500 <= response.status_code < 600 and attempt < MAX_ATTEMPTS
                )
                if not retry_server_error:
                    _raise_for_non_2xx(response)
                    names = response.text.splitlines()
                    if not names:
                        raise ValueError("delta index is empty")
                    return _sort_and_validate_names(names)
            if retry_server_error:
                _sleep_before_retry(attempt)
                continue
        except RETRYABLE_NETWORK_ERRORS:
            if attempt == MAX_ATTEMPTS:
                raise
            _sleep_before_retry(attempt)
    raise AssertionError("retry loop completed without returning or raising")


def download_file(
    session: requests.Session,
    file_name: str,
    data_dir: str | Path,
    base_url: str = DEFAULT_BASE_URL,
    downloaded_names: set[str] | None = None,
) -> Path:
    """Download one missing delta through a temporary file and atomic rename."""
    parse_source_file_name(file_name)
    _set_user_agent(session)
    destination_dir = Path(data_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    final_path = destination_dir / file_name
    part_path = destination_dir / f"{file_name}.part"

    if final_path.is_file():
        try:
            _validate_gzip(final_path, file_name)
        except DownloadIntegrityError:
            # Keep the damaged file in place until a valid replacement is ready.
            pass
        else:
            return final_path
    elif final_path.exists():
        raise FileExistsError(f"download destination is not a file: {final_path}")

    url = f"{base_url.rstrip('/')}/{file_name}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        part_path.unlink(missing_ok=True)
        try:
            response = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            with response:
                if 500 <= response.status_code < 600:
                    if attempt == MAX_ATTEMPTS:
                        _raise_for_non_2xx(response)
                    retry_server_error = True
                else:
                    retry_server_error = False
                    _raise_for_non_2xx(response)

                    expected_length = response.headers.get("Content-Length")
                    if expected_length is not None:
                        try:
                            expected_bytes = int(expected_length)
                        except ValueError as error:
                            raise ValueError(
                                f"invalid Content-Length for {file_name}: "
                                f"{expected_length!r}"
                            ) from error
                        if expected_bytes < 0:
                            raise ValueError(
                                f"invalid Content-Length for {file_name}: "
                                f"{expected_length!r}"
                            )

                    bytes_written = 0
                    with part_path.open("wb") as target:
                        for chunk in response.iter_content(DOWNLOAD_CHUNK_SIZE):
                            if chunk:
                                target.write(chunk)
                                bytes_written += len(chunk)

                    if expected_length is not None and bytes_written != expected_bytes:
                        raise IOError(
                            f"Content-Length mismatch for {file_name}: expected "
                            f"{expected_bytes} bytes, wrote {bytes_written}"
                        )
                    _validate_gzip(part_path, file_name)
                    os.replace(part_path, final_path)
                    if downloaded_names is not None:
                        downloaded_names.add(file_name)
                    return final_path
            if retry_server_error:
                _sleep_before_retry(attempt)
                continue
        except RETRYABLE_NETWORK_ERRORS:
            part_path.unlink(missing_ok=True)
            if attempt == MAX_ATTEMPTS:
                raise
            _sleep_before_retry(attempt)
        except Exception:
            part_path.unlink(missing_ok=True)
            raise

    raise AssertionError("retry loop completed without returning or raising")


def ensure_files(
    session: requests.Session,
    data_dir: str | Path,
    names: Iterable[str] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    downloaded_names: set[str] | None = None,
) -> list[Path]:
    """Ensure selected index files exist locally and return them oldest first."""
    selected_names = (
        fetch_index(session, base_url)
        if names is None
        else _sort_and_validate_names(names)
    )
    return [
        download_file(
            session,
            name,
            data_dir,
            base_url,
            downloaded_names,
        )
        for name in selected_names
    ]


def _positive_count(value: str) -> int:
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("--count must be greater than zero")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file_names", nargs="*", help="exact filenames to download")
    parser.add_argument(
        "--count",
        type=_positive_count,
        help="download only the N most recent index entries",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="delta directory (default: $OFF_DATA_DIR, else the project's data/)",
    )
    args = parser.parse_args()
    if args.count is not None and args.file_names:
        parser.error("--count cannot be combined with explicit filenames")
    if args.data_dir is None:
        args.data_dir = default_data_dir()

    with requests.Session() as session:
        index_names = fetch_index(session, DEFAULT_BASE_URL)
        if args.file_names:
            selected_names = _sort_and_validate_names(args.file_names)
        elif args.count is not None:
            selected_names = index_names[-args.count :]
        else:
            selected_names = index_names

        downloaded_names: set[str] = set()
        paths = ensure_files(
            session,
            args.data_dir,
            selected_names,
            base_url=DEFAULT_BASE_URL,
            downloaded_names=downloaded_names,
        )

    print(f"index entries: {len(index_names):,}")
    print(f"reused locally: {len(paths) - len(downloaded_names):,}")
    print(f"downloaded: {len(downloaded_names):,}")


if __name__ == "__main__":
    main()
