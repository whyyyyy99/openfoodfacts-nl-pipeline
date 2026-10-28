from __future__ import annotations

import ast
import inspect
import py_compile
from pathlib import Path

import pytest

from src.load import FileLoadError, LoadStateError
from src.transform import DataQualityError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = PROJECT_ROOT / "dags" / "openfoodfacts_daily.py"


def test_dag_has_valid_python_syntax(tmp_path):
    py_compile.compile(
        str(DAG_PATH),
        cfile=str(tmp_path / "openfoodfacts_daily.pyc"),
        doraise=True,
    )


def test_dag_runs_daily_at_0800_utc():
    module = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    dag_decorator = next(
        decorator
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "openfoodfacts_daily"
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
    )
    keyword_values = {keyword.arg: keyword.value for keyword in dag_decorator.keywords}

    assert ast.literal_eval(keyword_values["schedule"]) == "0 8 * * *"
    assert ast.literal_eval(keyword_values["catchup"]) is False
    assert ast.literal_eval(keyword_values["max_active_runs"]) == 1


def _load_dag():
    pytest.importorskip("airflow", reason="Airflow is intentionally optional")
    from airflow.models import DagBag

    # Airflow 3 removed the include_examples argument.
    options = (
        {"include_examples": False}
        if "include_examples" in inspect.signature(DagBag.__init__).parameters
        else {}
    )
    dag_bag = DagBag(dag_folder=str(DAG_PATH.parent), **options)
    assert dag_bag.import_errors == {}
    dag = dag_bag.get_dag("openfoodfacts_daily")
    assert dag is not None
    return dag


def _task_callable(task_id, monkeypatch, **replacements):
    """Return a task's Python callable with its module-level dependencies faked."""
    task_callable = _load_dag().get_task(task_id).python_callable
    for name, replacement in replacements.items():
        monkeypatch.setitem(task_callable.__globals__, name, replacement)
    return task_callable, task_callable.__globals__["AirflowFailException"]


def _raise(error):
    def fail(*_args, **_kwargs):
        raise error

    return fail


def test_dag_structure_when_airflow_is_installed():
    dag = _load_dag()
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # Airflow 2 exposes schedule_interval; Airflow 3 exposes schedule.
    schedule = getattr(dag, "schedule", None) or getattr(dag, "schedule_interval")
    assert str(schedule) == "0 8 * * *"
    assert set(dag.task_ids) == {
        "bootstrap_schema",
        "extract",
        "check_source_gap",
        "load",
        "transform",
    }
    assert dag.get_task("bootstrap_schema").downstream_task_ids == {"extract"}
    assert dag.get_task("extract").downstream_task_ids == {
        "check_source_gap",
        "load",
    }
    assert dag.get_task("check_source_gap").downstream_task_ids == {"load"}
    assert dag.get_task("load").downstream_task_ids == {"transform"}
    for task_id in dag.task_ids:
        assert dag.get_task(task_id).execution_timeout is not None
    assert dag.params["full_refresh"] is False


@pytest.mark.parametrize(
    "error",
    [FileLoadError("line 7 is not valid JSON"), LoadStateError("residual rows")],
)
def test_load_task_fails_fast_on_deterministic_load_errors(error, monkeypatch):
    load, fail_fast = _task_callable("load", monkeypatch, load_raw=_raise(error))

    with pytest.raises(fail_fast, match=str(error)):
        load(["openfoodfacts_products_100_200.json.gz"])


def test_transform_task_fails_fast_on_data_quality_error(monkeypatch):
    transform, fail_fast = _task_callable(
        "transform",
        monkeypatch,
        transform_and_refresh_mart=_raise(DataQualityError("repeated version key")),
    )

    with pytest.raises(fail_fast, match="repeated version key"):
        transform(params={}, ds="2026-09-18")


@pytest.mark.parametrize(
    ("task_id", "dependency", "kwargs"),
    [
        ("load", "load_raw", {"file_names": ["openfoodfacts_products_1_2.json.gz"]}),
        (
            "transform",
            "transform_and_refresh_mart",
            {"params": {}, "ds": "2026-09-18"},
        ),
    ],
)
def test_transient_errors_propagate_for_airflow_retries(
    task_id, dependency, kwargs, monkeypatch
):
    task_callable, fail_fast = _task_callable(
        task_id, monkeypatch, **{dependency: _raise(ConnectionError("db unavailable"))}
    )

    with pytest.raises(ConnectionError) as raised:
        task_callable(**kwargs)
    assert not isinstance(raised.value, fail_fast)


@pytest.mark.parametrize(
    ("params", "ds", "expected"),
    [
        ({}, "2026-09-18", False),  # Friday
        ({"full_refresh": True}, "2026-09-18", True),
        ({}, "2026-09-20", True),  # Sunday
    ],
)
def test_transform_task_chooses_full_refresh(params, ds, expected, monkeypatch):
    received = []

    def fake_refresh(*, full_refresh):
        received.append(full_refresh)
        return {"staged_rows": 0, "mart_rows": 0, "full_refresh": full_refresh}

    transform, _ = _task_callable(
        "transform", monkeypatch, transform_and_refresh_mart=fake_refresh
    )

    transform(params=params, ds=ds)

    assert received == [expected]
