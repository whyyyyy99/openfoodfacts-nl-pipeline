from __future__ import annotations

from pathlib import Path

import pytest

from src.load import LoadResult
from src.pipeline import DatabaseEvidence, _database_evidence, run_pipeline


OLDER = "openfoodfacts_products_9_10.json.gz"
NEWER = "openfoodfacts_products_10_11.json.gz"


class _Session:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _CountResult:
    def fetchone(self):
        return ([OLDER, NEWER], 20, 20, 18, 2, 6, 12, 4)


class _Connection:
    def execute(self, _query):
        return _CountResult()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_pipeline_runs_stages_in_order_and_reports_evidence(tmp_path, monkeypatch):
    events = []
    older_path = tmp_path / OLDER
    newer_path = tmp_path / NEWER
    older_path.write_bytes(b"existing")

    monkeypatch.setattr("src.pipeline.requests.Session", _Session)
    monkeypatch.setattr(
        "src.pipeline.apply_schema", lambda: events.append("schema")
    )

    def fake_fetch_index(_session, base_url):
        events.append(("index", base_url))
        return [OLDER, NEWER]

    def fake_ensure_files(
        _session, data_dir, names, *, base_url, downloaded_names
    ):
        events.append(("extract", Path(data_dir), list(names), base_url))
        newer_path.write_bytes(b"downloaded")
        downloaded_names.add(NEWER)
        return [older_path, newer_path]

    def fake_run_load(paths):
        events.append(("load", list(paths)))
        return [
            LoadResult(OLDER, 10, skipped=True),
            LoadResult(NEWER, 10, skipped=False),
        ]

    def fake_run_transform(*, rebuild):
        events.append(("transform", rebuild))
        return (10, 12)

    monkeypatch.setattr("src.pipeline.fetch_index", fake_fetch_index)
    monkeypatch.setattr("src.pipeline.ensure_files", fake_ensure_files)
    monkeypatch.setattr("src.pipeline.run_load", fake_run_load)
    monkeypatch.setattr("src.pipeline.run_transform", fake_run_transform)
    monkeypatch.setattr("src.pipeline.get_connection", lambda: _Connection())
    monkeypatch.setattr(
        "src.pipeline.table_checksum",
        lambda _connection, layer: f"{layer}-checksum",
    )

    result = run_pipeline(
        data_dir=tmp_path,
        count=2,
        base_url="https://example.test",
        verify=True,
    )

    assert events == [
        "schema",
        ("index", "https://example.test"),
        (
            "extract",
            tmp_path,
            [OLDER, NEWER],
            "https://example.test",
        ),
        ("load", [older_path, newer_path]),
        ("transform", False),
    ]
    assert result.selected_files == (OLDER, NEWER)
    assert result.loaded_source_files == (OLDER, NEWER)
    assert result.run_started_at.tzinfo is not None
    assert result.reused_file_count == 1
    assert result.downloaded_file_count == 1
    assert result.loaded_file_count == 1
    assert result.skipped_file_count == 1
    assert result.staged_row_count == 10
    assert (result.raw_row_count, result.staging_row_count, result.mart_row_count) == (
        20,
        20,
        12,
    )
    assert result.valid_staging_row_count == 18
    assert result.invalid_staging_row_count == 2
    assert result.non_winning_valid_row_count == 6
    assert result.cross_file_product_count == 4
    assert result.raw_checksum == "raw-checksum"
    assert result.staging_checksum == "staging-checksum"
    assert result.mart_checksum == "mart-checksum"


def test_explicit_local_files_do_not_fetch_index(tmp_path, monkeypatch):
    path = tmp_path / OLDER
    path.write_bytes(b"existing")
    events = []

    monkeypatch.setattr("src.pipeline.requests.Session", _Session)
    monkeypatch.setattr("src.pipeline.apply_schema", lambda: None)

    def unexpected_fetch(*_args):
        raise AssertionError("explicit filenames must not fetch the live index")

    def fake_ensure_files(
        _session, data_dir, names, *, base_url, downloaded_names
    ):
        events.append((Path(data_dir), list(names), base_url))
        return [path]

    monkeypatch.setattr("src.pipeline.fetch_index", unexpected_fetch)
    monkeypatch.setattr("src.pipeline.ensure_files", fake_ensure_files)
    monkeypatch.setattr(
        "src.pipeline.run_load",
        lambda _paths: [LoadResult(OLDER, 10, skipped=True)],
    )
    monkeypatch.setattr(
        "src.pipeline.run_transform", lambda *, rebuild: (0, 8)
    )
    monkeypatch.setattr(
        "src.pipeline._database_evidence",
        lambda *, verify: DatabaseEvidence(
            loaded_source_files=(OLDER,),
            raw_row_count=10,
            staging_row_count=10,
            valid_staging_row_count=8,
            invalid_staging_row_count=2,
            non_winning_valid_row_count=0,
            mart_row_count=8,
            cross_file_product_count=0,
            raw_checksum=None,
            staging_checksum="staging-checksum",
            mart_checksum="mart-checksum",
        ),
    )

    result = run_pipeline(
        data_dir=tmp_path,
        file_names=[OLDER],
        base_url="https://unavailable.test",
    )

    assert events == [(tmp_path, [OLDER], "https://unavailable.test")]
    assert result.selected_files == (OLDER,)


def test_default_run_skips_only_raw_checksum(monkeypatch):
    checksum_calls = []
    monkeypatch.setattr("src.pipeline.get_connection", lambda: _Connection())

    def fake_table_checksum(_connection, layer):
        checksum_calls.append(layer)
        return f"{layer}-checksum"

    monkeypatch.setattr("src.pipeline.table_checksum", fake_table_checksum)

    evidence = _database_evidence(verify=False)

    assert evidence == DatabaseEvidence(
        loaded_source_files=(OLDER, NEWER),
        raw_row_count=20,
        staging_row_count=20,
        valid_staging_row_count=18,
        invalid_staging_row_count=2,
        non_winning_valid_row_count=6,
        mart_row_count=12,
        cross_file_product_count=4,
        raw_checksum=None,
        staging_checksum="staging-checksum",
        mart_checksum="mart-checksum",
    )
    assert checksum_calls == ["staging", "mart"]


def test_pipeline_does_not_transform_after_load_failure(tmp_path, monkeypatch):
    path = tmp_path / OLDER
    calls = []
    monkeypatch.setattr("src.pipeline.requests.Session", _Session)
    monkeypatch.setattr("src.pipeline.apply_schema", lambda: None)
    monkeypatch.setattr("src.pipeline.fetch_index", lambda *_args: [OLDER])
    monkeypatch.setattr(
        "src.pipeline.ensure_files", lambda *_args, **_kwargs: [path]
    )

    def fail_load(_paths):
        raise RuntimeError("load failed")

    monkeypatch.setattr("src.pipeline.run_load", fail_load)
    monkeypatch.setattr(
        "src.pipeline.run_transform",
        lambda *, rebuild: calls.append(("transform", rebuild)),
    )
    monkeypatch.setattr(
        "src.pipeline._database_evidence", lambda **_kwargs: calls.append("evidence")
    )

    with pytest.raises(RuntimeError, match="load failed"):
        run_pipeline(data_dir=tmp_path)

    assert calls == []


def test_pipeline_does_not_collect_evidence_after_transform_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / OLDER
    calls = []
    monkeypatch.setattr("src.pipeline.requests.Session", _Session)
    monkeypatch.setattr("src.pipeline.apply_schema", lambda: None)
    monkeypatch.setattr("src.pipeline.fetch_index", lambda *_args: [OLDER])
    monkeypatch.setattr(
        "src.pipeline.ensure_files", lambda *_args, **_kwargs: [path]
    )
    monkeypatch.setattr(
        "src.pipeline.run_load",
        lambda _paths: [LoadResult(OLDER, 10, skipped=False)],
    )

    def fail_transform(*, rebuild):
        raise RuntimeError("transform failed")

    monkeypatch.setattr("src.pipeline.run_transform", fail_transform)
    monkeypatch.setattr(
        "src.pipeline._database_evidence", lambda **_kwargs: calls.append("evidence")
    )

    with pytest.raises(RuntimeError, match="transform failed"):
        run_pipeline(data_dir=tmp_path)

    assert calls == []


def test_pipeline_forwards_rebuild_to_transform(tmp_path, monkeypatch):
    path = tmp_path / OLDER
    path.write_bytes(b"existing")
    received = []
    monkeypatch.setattr("src.pipeline.requests.Session", _Session)
    monkeypatch.setattr("src.pipeline.apply_schema", lambda: None)
    monkeypatch.setattr(
        "src.pipeline.ensure_files", lambda *_args, **_kwargs: [path]
    )
    monkeypatch.setattr(
        "src.pipeline.run_load",
        lambda _paths: [LoadResult(OLDER, 10, skipped=True)],
    )
    monkeypatch.setattr(
        "src.pipeline.run_transform",
        lambda *, rebuild: (received.append(rebuild) or (10, 8)),
    )
    monkeypatch.setattr(
        "src.pipeline._database_evidence",
        lambda *, verify: DatabaseEvidence(
            loaded_source_files=(OLDER,),
            raw_row_count=10,
            staging_row_count=10,
            valid_staging_row_count=8,
            invalid_staging_row_count=2,
            non_winning_valid_row_count=0,
            mart_row_count=8,
            cross_file_product_count=0,
            raw_checksum=None,
            staging_checksum="staging-checksum",
            mart_checksum="mart-checksum",
        ),
    )

    run_pipeline(data_dir=tmp_path, file_names=[OLDER], rebuild=True)

    assert received == [True]


@pytest.mark.parametrize(
    ("count", "file_names", "message"),
    [
        (0, None, "greater than zero"),
        (1, [OLDER], "cannot be combined"),
    ],
)
def test_pipeline_rejects_invalid_selection_before_side_effects(
    count, file_names, message, monkeypatch
):
    calls = []
    monkeypatch.setattr("src.pipeline.apply_schema", lambda: calls.append("schema"))

    with pytest.raises(ValueError, match=message):
        run_pipeline(count=count, file_names=file_names)

    assert calls == []
