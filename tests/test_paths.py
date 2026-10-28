from pathlib import Path

from src.airflow_tasks import extract_live_deltas, load_raw
from src.extract import DATA_DIR_ENV, PROJECT_ROOT, default_data_dir
from src.load import LoadResult


NAME = "openfoodfacts_products_100_200.json.gz"


class _Session:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_default_data_dir_is_project_data_regardless_of_working_directory(
    tmp_path, monkeypatch
):
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)
    monkeypatch.chdir(tmp_path)

    assert default_data_dir() == PROJECT_ROOT / "data"
    assert default_data_dir().is_absolute()


def test_data_dir_environment_variable_overrides_default(tmp_path, monkeypatch):
    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "shared"))

    assert default_data_dir() == tmp_path / "shared"


def test_airflow_tasks_resolve_data_dir_at_call_time(tmp_path, monkeypatch):
    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path))
    received = {}

    monkeypatch.setattr("src.airflow_tasks.requests.Session", _Session)
    monkeypatch.setattr("src.airflow_tasks.fetch_index", lambda *_args: [NAME])

    def fake_ensure_files(_session, data_dir, names, *, base_url):
        received["extract"] = Path(data_dir)
        return [Path(data_dir) / name for name in names]

    def fake_run_load(paths):
        received["load"] = list(paths)
        return [LoadResult(NAME, 1, skipped=False)]

    monkeypatch.setattr("src.airflow_tasks.ensure_files", fake_ensure_files)
    monkeypatch.setattr("src.airflow_tasks.run_load", fake_run_load)

    extract_live_deltas()
    load_raw([NAME])

    assert received == {"extract": tmp_path, "load": [tmp_path / NAME]}
