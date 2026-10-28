from __future__ import annotations

import gzip
import os
import zlib
from pathlib import Path

import pytest
import requests

from src.extract import (
    DownloadIntegrityError,
    MAX_ATTEMPTS,
    REQUEST_TIMEOUT,
    USER_AGENT,
    download_file,
    ensure_files,
    fetch_index,
)


BASE_URL = "https://example.test/delta"
OLDER = "openfoodfacts_products_9_10.json.gz"
NEWER = "openfoodfacts_products_10_11.json.gz"


def _midstream_corrupt_gzip() -> bytes:
    raw = b"".join(
        f'{{"code":"{index:06d}","noise":"{index * index:012d}"}}\n'.encode()
        for index in range(1_000)
    )
    compressed = bytearray(gzip.compress(raw))
    compressed[2_000:2_064] = bytes(
        byte ^ 0xFF for byte in compressed[2_000:2_064]
    )
    with pytest.raises(zlib.error):
        gzip.decompress(compressed)
    return bytes(compressed)


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        chunks: list[bytes | Exception] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.chunks = chunks or []
        self.headers = headers or {}
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, _chunk_size: int):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk


class FakeSession:
    def __init__(self, effects=None) -> None:
        self.effects = list(effects or [])
        self.calls = []
        self.headers = {}

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.effects:
            raise AssertionError(f"unexpected request: {url}")
        effect = self.effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def test_fetch_index_validates_and_sorts_by_integer_timestamp():
    session = FakeSession([FakeResponse(text=f"{NEWER}\n{OLDER}\n")])

    names = fetch_index(session, BASE_URL)  # type: ignore[arg-type]

    assert names == [OLDER, NEWER]
    assert session.calls == [(f"{BASE_URL}/index.txt", {"timeout": REQUEST_TIMEOUT})]
    assert session.headers["User-Agent"] == USER_AGENT


@pytest.mark.parametrize(
    "bad_name",
    [
        "products.json.gz",
        "../openfoodfacts_products_9_10.json.gz",
        "folder/openfoodfacts_products_9_10.json.gz",
        r"folder\openfoodfacts_products_9_10.json.gz",
        "..",
    ],
)
def test_fetch_index_rejects_malformed_or_path_traversing_entry(bad_name):
    session = FakeSession([FakeResponse(text=f"{NEWER}\n{bad_name}\n")])

    with pytest.raises(ValueError, match="invalid Open Food Facts delta filename"):
        fetch_index(session, BASE_URL)  # type: ignore[arg-type]


def test_download_file_reuses_existing_file_without_request(tmp_path):
    existing = tmp_path / OLDER
    content = gzip.compress(b'{"code":"00123"}\n')
    existing.write_bytes(content)
    session = FakeSession()

    result = download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert result == existing
    assert existing.read_bytes() == content
    assert session.calls == []


def test_corrupt_existing_file_is_atomically_replaced(tmp_path):
    existing = tmp_path / OLDER
    existing.write_bytes(b"corrupt-local-copy")
    replacement = gzip.compress(b'{"code":"00123"}\n')
    session = FakeSession([FakeResponse(chunks=[replacement])])
    downloaded_names: set[str] = set()

    result = download_file(  # type: ignore[arg-type]
        session,
        OLDER,
        tmp_path,
        base_url=BASE_URL,
        downloaded_names=downloaded_names,
    )

    assert result == existing
    assert existing.read_bytes() == replacement
    assert downloaded_names == {OLDER}
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_midstream_zlib_corruption_is_replaced(tmp_path):
    existing = tmp_path / OLDER
    existing.write_bytes(_midstream_corrupt_gzip())
    replacement = gzip.compress(b'{"code":"00123"}\n')
    session = FakeSession([FakeResponse(chunks=[replacement])])

    result = download_file(  # type: ignore[arg-type]
        session,
        OLDER,
        tmp_path,
        base_url=BASE_URL,
    )

    assert result == existing
    assert existing.read_bytes() == replacement
    assert len(session.calls) == 1
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_failed_repair_preserves_corrupt_existing_file(tmp_path):
    existing = tmp_path / OLDER
    original = b"corrupt-local-copy"
    existing.write_bytes(original)
    session = FakeSession(
        [FakeResponse(chunks=[b"partial", RuntimeError("stream failed")])]
    )

    with pytest.raises(RuntimeError, match="stream failed"):
        download_file(  # type: ignore[arg-type]
            session,
            OLDER,
            tmp_path,
            base_url=BASE_URL,
        )

    assert existing.read_bytes() == original
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_successful_download_uses_part_file_and_atomic_replace(tmp_path, monkeypatch):
    content = gzip.compress(b'{"code":"00123"}\n')
    session = FakeSession(
        [
            FakeResponse(
                chunks=[content[:5], content[5:]],
                headers={"Content-Length": str(len(content))},
            )
        ]
    )
    replace_calls = []
    real_replace = os.replace

    def recording_replace(source, destination):
        replace_calls.append((Path(source), Path(destination)))
        assert Path(source).read_bytes() == content
        real_replace(source, destination)

    monkeypatch.setattr("src.extract.os.replace", recording_replace)

    result = download_file(  # type: ignore[arg-type]
        session, OLDER, tmp_path, base_url=BASE_URL
    )

    part_path = tmp_path / f"{OLDER}.part"
    assert result.read_bytes() == content
    assert replace_calls == [(part_path, tmp_path / OLDER)]
    assert not part_path.exists()
    assert session.calls[0][1] == {"stream": True, "timeout": REQUEST_TIMEOUT}
    assert session.calls[0][0] == f"{BASE_URL}/{OLDER}"
    assert session.headers["User-Agent"] == USER_AGENT


def test_failed_download_removes_final_and_part_files(tmp_path):
    session = FakeSession(
        [FakeResponse(chunks=[b"partial", RuntimeError("stream failed")])]
    )

    with pytest.raises(RuntimeError, match="stream failed"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert not (tmp_path / OLDER).exists()
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_content_length_mismatch_fails_and_cleans_up(tmp_path):
    session = FakeSession(
        [FakeResponse(chunks=[b"short"], headers={"Content-Length": "10"})]
    )

    with pytest.raises(IOError, match="expected 10 bytes, wrote 5"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert not (tmp_path / OLDER).exists()
    assert not (tmp_path / f"{OLDER}.part").exists()


@pytest.mark.parametrize("invalid_body", [b"only-half-the-file", b"<html>error</html>"])
def test_invalid_gzip_without_content_length_fails_and_cleans_up(
    invalid_body, tmp_path
):
    session = FakeSession([FakeResponse(chunks=[invalid_body])])

    with pytest.raises(DownloadIntegrityError, match="complete valid gzip"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert not (tmp_path / OLDER).exists()
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_valid_gzip_without_content_length_is_accepted(tmp_path):
    content = gzip.compress(b'{"code":"00123"}\n')
    session = FakeSession([FakeResponse(chunks=[content])])

    result = download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert result.read_bytes() == content


def test_timeout_is_retried_then_raised(tmp_path, monkeypatch):
    session = FakeSession([requests.Timeout("slow")] * MAX_ATTEMPTS)
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    with pytest.raises(requests.Timeout, match="slow"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert len(session.calls) == MAX_ATTEMPTS
    assert sleeps == [1, 2]
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_connection_error_is_retried_then_raised(tmp_path, monkeypatch):
    session = FakeSession([requests.ConnectionError("reset")] * MAX_ATTEMPTS)
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    with pytest.raises(requests.ConnectionError, match="reset"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert len(session.calls) == MAX_ATTEMPTS
    assert sleeps == [1, 2]
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_stream_connection_error_cleans_up_and_retries(tmp_path, monkeypatch):
    content = gzip.compress(b'{"code":"00123"}\n')
    session = FakeSession(
        [
            FakeResponse(chunks=[b"partial", requests.ConnectionError("reset")]),
            FakeResponse(chunks=[content]),
        ]
    )
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    result = download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert result.read_bytes() == content
    assert len(session.calls) == 2
    assert sleeps == [1]
    assert not (tmp_path / f"{OLDER}.part").exists()


def test_fetch_index_retries_connection_error(monkeypatch):
    session = FakeSession(
        [
            requests.ConnectionError("dns"),
            FakeResponse(text=f"{NEWER}\n{OLDER}\n"),
        ]
    )
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    assert fetch_index(session, BASE_URL) == [OLDER, NEWER]  # type: ignore[arg-type]
    assert len(session.calls) == 2
    assert sleeps == [1]


def test_5xx_is_retried_then_raised(tmp_path, monkeypatch):
    responses = [FakeResponse(status_code=503) for _ in range(MAX_ATTEMPTS)]
    session = FakeSession(responses)
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    with pytest.raises(requests.HTTPError, match="503"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert len(session.calls) == MAX_ATTEMPTS
    assert sleeps == [1, 2]
    assert all(response.closed for response in responses)


def test_404_raises_immediately_without_retry(tmp_path, monkeypatch):
    response = FakeResponse(status_code=404)
    session = FakeSession([response])
    sleeps = []
    monkeypatch.setattr("src.extract.time.sleep", sleeps.append)

    with pytest.raises(requests.HTTPError, match="404"):
        download_file(session, OLDER, tmp_path)  # type: ignore[arg-type]

    assert len(session.calls) == 1
    assert sleeps == []
    assert response.closed


def test_ensure_files_defaults_to_index_and_returns_oldest_first(tmp_path):
    content = gzip.compress(b'{"code":"00456"}\n')
    existing = tmp_path / OLDER
    existing.write_bytes(gzip.compress(b'{"code":"00123"}\n'))
    session = FakeSession(
        [
            FakeResponse(text=f"{NEWER}\n{OLDER}\n"),
            FakeResponse(
                chunks=[content], headers={"Content-Length": str(len(content))}
            ),
        ]
    )

    paths = ensure_files(  # type: ignore[arg-type]
        session, tmp_path, base_url=BASE_URL
    )

    assert paths == [existing, tmp_path / NEWER]
    assert (tmp_path / NEWER).read_bytes() == content
    assert len(session.calls) == 2
    assert session.calls[0][0] == f"{BASE_URL}/index.txt"
    assert session.calls[1][0] == f"{BASE_URL}/{NEWER}"
