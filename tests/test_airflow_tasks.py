from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pytest

from src.airflow_tasks import (
    apply_schema_task,
    detect_source_gap,
    extract_live_deltas,
    load_raw,
    should_full_refresh,
    transform_and_refresh_mart,
)
from src.load import LoadResult
from src.transform import DataQualityError


OLD = "openfoodfacts_products_100_200.json.gz"
NEW = "openfoodfacts_products_200_300.json.gz"


class _Session:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_apply_schema_task_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr("src.airflow_tasks.apply_schema", lambda: calls.append(True))

    apply_schema_task()

    assert calls == [True]


def test_extract_returns_all_selected_files_including_existing(tmp_path, monkeypatch):
    existing = tmp_path / OLD
    downloaded = tmp_path / NEW
    existing.write_bytes(b"already here")
    events = []

    monkeypatch.setattr("src.airflow_tasks.requests.Session", _Session)
    monkeypatch.setattr(
        "src.airflow_tasks.fetch_index",
        lambda _session, base_url: [OLD, NEW],
    )

    def fake_ensure_files(_session, data_dir, names, *, base_url):
        events.append((Path(data_dir), list(names), base_url))
        downloaded.write_bytes(b"downloaded")
        return [existing, downloaded]

    monkeypatch.setattr("src.airflow_tasks.ensure_files", fake_ensure_files)

    result = extract_live_deltas(tmp_path, "https://example.test")

    assert result == [OLD, NEW]
    assert events == [(tmp_path, [OLD, NEW], "https://example.test")]


def test_extract_does_not_swallow_upstream_failure(monkeypatch):
    monkeypatch.setattr("src.airflow_tasks.requests.Session", _Session)

    def fail(*_args):
        raise ConnectionError("network unavailable")

    monkeypatch.setattr("src.airflow_tasks.fetch_index", fail)

    with pytest.raises(ConnectionError, match="network unavailable"):
        extract_live_deltas()


def test_gap_check_skips_initial_load(monkeypatch, caplog):
    monkeypatch.setattr("src.airflow_tasks._loaded_source_file_names", lambda: [])

    with caplog.at_level(logging.INFO):
        result = detect_source_gap([OLD, NEW])

    assert result is None
    assert "initial load" in caplog.text


@pytest.mark.parametrize(
    ("loaded_name", "expected_message"),
    [
        ("openfoodfacts_products_1_100.json.gz", "remain recoverable"),
        ("openfoodfacts_products_1_150.json.gz", "remain recoverable"),
    ],
)
def test_gap_check_does_not_warn_for_touching_or_overlapping_windows(
    loaded_name, expected_message, monkeypatch, caplog
):
    monkeypatch.setattr(
        "src.airflow_tasks._loaded_source_file_names", lambda: [loaded_name]
    )

    with caplog.at_level(logging.INFO):
        result = detect_source_gap([OLD, NEW])

    assert result is None
    assert expected_message in caplog.text
    assert "Potential unrecoverable" not in caplog.text


def test_gap_check_warns_without_failing(monkeypatch, caplog):
    loaded_name = "openfoodfacts_products_1_90.json.gz"
    monkeypatch.setattr(
        "src.airflow_tasks._loaded_source_file_names", lambda: [loaded_name]
    )

    with caplog.at_level(logging.WARNING):
        result = detect_source_gap([OLD, NEW])

    assert result == {
        "latest_loaded_file": loaded_name,
        "latest_loaded_window_end": 90,
        "oldest_available_file": OLD,
        "oldest_available_window_start": 100,
        "gap_seconds": 10,
    }
    assert "Potential unrecoverable source-window gap" in caplog.text


def test_gap_check_ignores_non_window_names_and_uses_parseable_history(
    monkeypatch, caplog
):
    monkeypatch.setattr(
        "src.airflow_tasks._loaded_source_file_names",
        lambda: ["my_manual_backfill.json.gz", "openfoodfacts_products_1_100.json.gz"],
    )

    with caplog.at_level(logging.INFO):
        result = detect_source_gap([OLD, NEW])

    assert result is None
    assert "ignored loaded source files" in caplog.text
    assert "my_manual_backfill.json.gz" in caplog.text
    assert "remain recoverable" in caplog.text


def test_gap_check_warns_and_continues_when_no_loaded_name_has_a_window(
    monkeypatch, caplog
):
    monkeypatch.setattr(
        "src.airflow_tasks._loaded_source_file_names",
        lambda: ["my_manual_backfill.json.gz"],
    )

    with caplog.at_level(logging.WARNING):
        result = detect_source_gap([OLD, NEW])

    assert result is None
    assert "my_manual_backfill.json.gz" in caplog.text
    assert "no loaded source file has a parseable window" in caplog.text


def test_load_raw_uses_only_selected_filenames(tmp_path, monkeypatch):
    extra = tmp_path / "openfoodfacts_products_300_400.json.gz"
    extra.write_bytes(b"must not be loaded")
    received = []

    def fake_run_load(paths):
        received.extend(paths)
        return [
            LoadResult(OLD, 10, skipped=True),
            LoadResult(NEW, 12, skipped=False),
        ]

    monkeypatch.setattr("src.airflow_tasks.run_load", fake_run_load)

    result = load_raw([OLD, NEW], tmp_path)

    assert received == [tmp_path / OLD, tmp_path / NEW]
    assert extra not in received
    assert result == {
        "selected_files": 2,
        "loaded_files": 1,
        "skipped_files": 1,
        "loaded_rows": 12,
    }


def test_transform_preserves_data_quality_error(monkeypatch):
    def fail(**_kwargs):
        raise DataQualityError("duplicate product version")

    monkeypatch.setattr("src.airflow_tasks.run_transform", fail)

    with pytest.raises(DataQualityError, match="duplicate product version"):
        transform_and_refresh_mart()


@pytest.mark.parametrize(
    ("requested", "run_date", "expected"),
    [
        (False, date(2026, 9, 14), False),  # Monday
        (False, date(2026, 9, 19), False),  # Saturday
        (False, date(2026, 9, 20), True),  # Sunday
        (True, date(2026, 9, 14), True),  # explicit request on any day
    ],
)
def test_full_refresh_is_weekly_or_explicitly_requested(requested, run_date, expected):
    assert should_full_refresh(requested=requested, run_date=run_date) is expected


def test_transform_forwards_full_refresh_to_run_transform(monkeypatch):
    received = []

    def fake_run_transform(*, full_refresh):
        received.append(full_refresh)
        return 3, 7

    monkeypatch.setattr("src.airflow_tasks.run_transform", fake_run_transform)

    assert transform_and_refresh_mart(full_refresh=True) == {
        "staged_rows": 3,
        "mart_rows": 7,
        "full_refresh": True,
    }
    assert received == [True]
