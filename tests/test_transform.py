from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.transform import (
    DataQualityError,
    EMPTY_PRODUCT_CODE,
    HEURISTIC_ENERGY_OVER_950KCAL,
    HEURISTIC_MACRONUTRIENT_SUM_OVER_105G,
    INVALID_LAST_MODIFIED_T,
    ALL_AFFECTED_CODES_SQL,
    ALL_RAW_RECORDS_SQL,
    COUNT_AFFECTED_CODES_SQL,
    LAYER_COUNTS_SQL,
    MARK_MART_APPLIED_SQL,
    MART_DELETE_AFFECTED_SQL,
    MART_FULL_REFRESH,
    MART_INCREMENTAL,
    MART_INSERT_SQL,
    MART_REFRESH_LOCK_SQL,
    SET_LOCK_TIMEOUT_SQL,
    MISSING_NUTRITION_BASIS,
    MISSING_PRODUCT_CODE,
    NON_DIGIT_PRODUCT_CODE,
    NUTRIENT_COMPARISON_TOLERANCE_G,
    NUTRIENT_EXCEEDS_100G,
    PENDING_AFFECTED_CODES_SQL,
    PENDING_RAW_RECORDS_SQL,
    REFRESH_LOG_INSERT_SQL,
    SATURATED_FAT_EXCEEDS_FAT,
    STREAM_BATCH_SIZE,
    SUGARS_EXCEED_CARBOHYDRATES,
    UNEXPECTED_NUTRITION_UNIT,
    UNEXPECTED_NUTRISCORE_VALUE,
    UNSUPPORTED_NUTRITION_BASIS,
    WINNER_CHECK_SQL,
    WITHIN_FILE_DUPLICATES_SQL,
    assert_staging_quality,
    assert_layer_reconciliation,
    refresh_mart,
    run_transform,
    stage_all_records,
    stage_pending_records,
    transform_payload,
)


LOADED_AT = datetime(2026, 9, 18, tzinfo=UTC)


def test_missing_last_modified_is_flagged_and_invalid():
    row = transform_payload("delta.json.gz", 1, LOADED_AT, {"code": "00123"})

    assert row.last_modified_at is None
    assert INVALID_LAST_MODIFIED_T in row.validation_errors


@pytest.mark.parametrize("value", [None, "1789000000", 1.5, True, 10**30])
def test_unusable_last_modified_is_flagged(value):
    row = transform_payload(
        "delta.json.gz", 1, LOADED_AT, {"code": "00123", "last_modified_t": value}
    )

    assert row.last_modified_at is None
    assert row.validation_errors == [INVALID_LAST_MODIFIED_T]


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        ({}, MISSING_PRODUCT_CODE),
        ({"code": None}, MISSING_PRODUCT_CODE),
        ({"code": ""}, EMPTY_PRODUCT_CODE),
        ({"code": "  "}, EMPTY_PRODUCT_CODE),
        ({"code": "12A"}, NON_DIGIT_PRODUCT_CODE),
        ({"code": 123}, NON_DIGIT_PRODUCT_CODE),
    ],
)
def test_product_code_validation_errors(payload, expected_error):
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        {**payload, "last_modified_t": 1_789_000_000},
    )

    assert expected_error in row.validation_errors


def test_valid_record_preserves_leading_zero_and_trims_whitespace():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        {"code": " 00123 ", "last_modified_t": 1_789_000_000},
    )

    assert row.product_code == "00123"
    assert row.last_modified_at is not None
    assert row.validation_errors == []


def test_blank_product_name_becomes_null():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        {"code": "00123", "last_modified_t": 1_789_000_000, "product_name": "  "},
    )

    assert row.product_name is None


def test_array_with_non_string_item_is_not_cast():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        {
            "code": "00123",
            "last_modified_t": 1_789_000_000,
            "brands_tags": ["brand", 7],
        },
    )

    assert row.brands_tags is None


@pytest.mark.parametrize(
    ("source_value", "grade", "status", "warning"),
    [
        ("a", "a", "available", None),
        ("unknown", None, "unknown", None),
        ("not-applicable", None, "not_applicable", None),
        (None, None, "missing", None),
        ("A", None, "unexpected", UNEXPECTED_NUTRISCORE_VALUE),
        (" a ", None, "unexpected", UNEXPECTED_NUTRISCORE_VALUE),
        ("", None, "unexpected", UNEXPECTED_NUTRISCORE_VALUE),
        ("future-value", None, "unexpected", UNEXPECTED_NUTRISCORE_VALUE),
    ],
)
def test_nutriscore_grade_and_status_are_separated(
    source_value, grade, status, warning
):
    payload = {"code": "00123", "last_modified_t": 1_789_000_000}
    if source_value is not None:
        payload["nutriscore_grade"] = source_value

    row = transform_payload("delta.json.gz", 1, LOADED_AT, payload)

    assert row.nutriscore_grade == grade
    assert row.nutriscore_status == status
    assert (warning in row.quality_warnings) if warning else not row.quality_warnings


def _nutrition_payload(per="100g", **nutrients):
    aggregated = {"nutrients": nutrients}
    if per is not None:
        aggregated["per"] = per
    return {
        "code": "00123",
        "last_modified_t": 1_789_000_000,
        "nutrition": {"aggregated_set": aggregated},
    }


def test_expected_nutrition_values_and_units_are_extracted():
    payload = _nutrition_payload(
        **{
            "energy-kcal": {"value": 120, "unit": "kcal"},
            "energy-kj": {"value": 502.1, "unit": "kJ"},
            "fat": {"value": 4.5, "unit": "g"},
            "proteins": {"value": 6, "unit": "g"},
            "carbohydrates": {"value": 20, "unit": "g"},
            "sugars": {"value": 8, "unit": "g"},
            "salt": {"value": 0.4, "unit": "g"},
            "saturated-fat": {"value": 1.5, "unit": "g"},
        }
    )

    row = transform_payload("delta.json.gz", 1, LOADED_AT, payload)

    assert row.energy_kcal == Decimal("120")
    assert row.energy_kj == Decimal("502.1")
    assert row.fat_g == Decimal("4.5")
    assert row.proteins_g == Decimal("6")
    assert row.carbohydrates_g == Decimal("20")
    assert row.sugars_g == Decimal("8")
    assert row.salt_g == Decimal("0.4")
    assert row.saturated_fat_g == Decimal("1.5")
    assert row.quality_warnings == []


@pytest.mark.parametrize("source_value", ["1.5", True])
def test_non_numeric_json_types_are_not_cast_as_nutrients(source_value):
    payload = _nutrition_payload(fat={"value": source_value, "unit": "g"})

    row = transform_payload("delta.json.gz", 1, LOADED_AT, payload)

    assert row.fat_g is None


@pytest.mark.parametrize(
    ("nutrient", "unit", "attribute"),
    [
        ("energy-kcal", "kJ", "energy_kcal"),
        ("energy-kj", "kcal", "energy_kj"),
        ("fat", "mg", "fat_g"),
        ("proteins", "mg", "proteins_g"),
        ("carbohydrates", "mg", "carbohydrates_g"),
        ("sugars", "mg", "sugars_g"),
        ("salt", "mg", "salt_g"),
        ("saturated-fat", "mg", "saturated_fat_g"),
    ],
)
def test_unexpected_nutrition_unit_is_null_and_warned(nutrient, unit, attribute):
    payload = _nutrition_payload(**{nutrient: {"value": 1, "unit": unit}})

    row = transform_payload("delta.json.gz", 1, LOADED_AT, payload)

    assert getattr(row, attribute) is None
    assert UNEXPECTED_NUTRITION_UNIT in row.quality_warnings


def test_unsupported_nutrition_basis_warns_without_invalidating_product():
    payload = {
        "code": "00123",
        "last_modified_t": 1_789_000_000,
        "nutrition": {"aggregated_set": {"per": "serving", "nutrients": {}}},
    }

    row = transform_payload("delta.json.gz", 1, LOADED_AT, payload)

    assert row.nutrition_basis is None
    assert row.validation_errors == []
    assert row.quality_warnings == [UNSUPPORTED_NUTRITION_BASIS]


def test_nutrients_without_basis_are_warned():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        _nutrition_payload(per=None, fat={"value": 1, "unit": "g"}),
    )

    assert row.nutrition_basis is None
    assert row.fat_g is None
    assert row.quality_warnings == [MISSING_NUTRITION_BASIS]


@pytest.mark.parametrize(
    ("nutrients", "warning"),
    [
        ({"fat": {"value": 100, "unit": "g"}}, None),
        ({"fat": {"value": 100.01, "unit": "g"}}, NUTRIENT_EXCEEDS_100G),
        (
            {
                "carbohydrates": {"value": 10, "unit": "g"},
                "sugars": {
                    "value": 10 + float(NUTRIENT_COMPARISON_TOLERANCE_G),
                    "unit": "g",
                },
            },
            None,
        ),
        (
            {
                "carbohydrates": {"value": 10, "unit": "g"},
                "sugars": {"value": 10.11, "unit": "g"},
            },
            SUGARS_EXCEED_CARBOHYDRATES,
        ),
        (
            {
                "fat": {"value": 10, "unit": "g"},
                "saturated-fat": {"value": 10.1, "unit": "g"},
            },
            None,
        ),
        (
            {
                "fat": {"value": 10, "unit": "g"},
                "saturated-fat": {"value": 10.11, "unit": "g"},
            },
            SATURATED_FAT_EXCEEDS_FAT,
        ),
        (
            {
                "fat": {"value": 35, "unit": "g"},
                "proteins": {"value": 20, "unit": "g"},
                "carbohydrates": {"value": 49, "unit": "g"},
                "salt": {"value": 1, "unit": "g"},
            },
            None,
        ),
        (
            {
                "fat": {"value": 35.01, "unit": "g"},
                "proteins": {"value": 20, "unit": "g"},
                "carbohydrates": {"value": 49, "unit": "g"},
                "salt": {"value": 1, "unit": "g"},
            },
            HEURISTIC_MACRONUTRIENT_SUM_OVER_105G,
        ),
        ({"energy-kcal": {"value": 950, "unit": "kcal"}}, None),
        (
            {"energy-kcal": {"value": 950.01, "unit": "kcal"}},
            HEURISTIC_ENERGY_OVER_950KCAL,
        ),
    ],
)
def test_nutrition_warning_boundaries(nutrients, warning):
    row = transform_payload(
        "delta.json.gz", 1, LOADED_AT, _nutrition_payload(**nutrients)
    )

    if warning is None:
        assert row.quality_warnings == []
    else:
        assert warning in row.quality_warnings


def test_100ml_basis_does_not_apply_100g_mass_limit():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        _nutrition_payload(per="100ml", fat={"value": 101, "unit": "g"}),
    )

    assert NUTRIENT_EXCEEDS_100G not in row.quality_warnings


@pytest.mark.parametrize(
    ("nutrients", "expected_warning"),
    [
        (
            {
                "carbohydrates": {"value": 10, "unit": "g"},
                "sugars": {"value": 10.11, "unit": "g"},
            },
            SUGARS_EXCEED_CARBOHYDRATES,
        ),
        (
            {
                "fat": {"value": 10, "unit": "g"},
                "saturated-fat": {"value": 10.11, "unit": "g"},
            },
            SATURATED_FAT_EXCEEDS_FAT,
        ),
    ],
)
def test_100ml_basis_applies_composition_consistency_checks(
    nutrients, expected_warning
):
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        _nutrition_payload(per="100ml", **nutrients),
    )

    assert expected_warning in row.quality_warnings


def test_100ml_basis_does_not_apply_100g_heuristics():
    row = transform_payload(
        "delta.json.gz",
        1,
        LOADED_AT,
        _nutrition_payload(
            per="100ml",
            **{
                "energy-kcal": {"value": 951, "unit": "kcal"},
                "fat": {"value": 40, "unit": "g"},
                "proteins": {"value": 30, "unit": "g"},
                "carbohydrates": {"value": 40, "unit": "g"},
            },
        ),
    )

    assert HEURISTIC_MACRONUTRIENT_SUM_OVER_105G not in row.quality_warnings
    assert HEURISTIC_ENERGY_OVER_950KCAL not in row.quality_warnings


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class _QualityConnection:
    def __init__(self, results):
        self._results = iter(results)

    def execute(self, _query):
        return _Result(next(self._results))


def test_duplicate_quality_check_fails_closed():
    connection = _QualityConnection(
        [[("delta.json.gz", "00123", 2)], [("00123", LOADED_AT, 2)]]
    )

    with pytest.raises(DataQualityError, match="within-file duplicate"):
        assert_staging_quality(connection)  # type: ignore[arg-type]


def test_winner_order_uses_timestamp_then_deterministic_provenance_fallback():
    order_by = MART_INSERT_SQL.split("ORDER BY", maxsplit=1)[1]

    assert "s.product_code" in order_by
    assert "s.last_modified_at DESC" in order_by
    assert "window_ended_at" not in order_by
    assert "s.source_file_name DESC" in order_by
    assert "s.source_line_number DESC" in order_by


class _RefreshResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class _RefreshConnection:
    """Record refresh statements and answer the ones whose results are read."""

    def __init__(self):
        self.queries = []
        self.log_params = None

    def execute(self, query, params=None):
        self.queries.append(query)
        if query == COUNT_AFFECTED_CODES_SQL:
            return _RefreshResult([(2,)])
        if query == LAYER_COUNTS_SQL:
            return _RefreshResult([(5, 5, 5, 0, 3, 3)])
        if query == WINNER_CHECK_SQL:
            return _RefreshResult([(0, 0)])
        if query == REFRESH_LOG_INSERT_SQL:
            self.log_params = params
            return _RefreshResult([(3,)])
        return _RefreshResult()


def test_incremental_refresh_scopes_checks_rewrite_and_marker_to_pending_codes():
    connection = _RefreshConnection()

    assert refresh_mart(connection) == 3  # type: ignore[arg-type]

    queries = connection.queries
    assert queries[:2] == [SET_LOCK_TIMEOUT_SQL, MART_REFRESH_LOCK_SQL]
    assert PENDING_AFFECTED_CODES_SQL in queries
    assert ALL_AFFECTED_CODES_SQL not in queries
    assert WINNER_CHECK_SQL in queries
    assert (
        queries.index(WITHIN_FILE_DUPLICATES_SQL)
        < queries.index(MART_DELETE_AFFECTED_SQL)
        < queries.index(MART_INSERT_SQL)
        < queries.index(LAYER_COUNTS_SQL)
        < queries.index(MARK_MART_APPLIED_SQL)
        < queries.index(REFRESH_LOG_INSERT_SQL)
    )
    assert connection.log_params == (MART_INCREMENTAL, 2)


def test_full_refresh_uses_the_same_statements_over_every_code():
    connection = _RefreshConnection()

    refresh_mart(connection, refresh_mode=MART_FULL_REFRESH)  # type: ignore[arg-type]

    assert ALL_AFFECTED_CODES_SQL in connection.queries
    assert PENDING_AFFECTED_CODES_SQL not in connection.queries
    assert WINNER_CHECK_SQL in connection.queries
    assert MART_DELETE_AFFECTED_SQL in connection.queries
    assert MART_INSERT_SQL in connection.queries
    assert connection.log_params == (MART_FULL_REFRESH, 2)


def _column_list(sql, after, before):
    block = sql.split(after, 1)[1].split(before, 1)[0]
    return [column.strip() for column in block.split(",")]


def test_mart_sql_files_keep_one_ordered_column_list():
    columns = _column_list(MART_INSERT_SQL, "INSERT INTO mart.products (", ")")
    qualified = {
        prefix: [f"{prefix}.{column}" for column in columns] for prefix in "sm"
    }

    assert len(columns) == len(set(columns)) == 23
    assert (
        _column_list(MART_INSERT_SQL, "DISTINCT ON (s.product_code)", "FROM")
        == qualified["s"]
    )
    assert (
        _column_list(WINNER_CHECK_SQL, "DISTINCT ON (s.product_code)", "FROM")
        == qualified["s"]
    )
    assert _column_list(WINNER_CHECK_SQL, "actual AS (\n    SELECT", "FROM") == (
        qualified["m"]
    )


def test_layer_reconciliation_fails_when_mart_winner_differs_from_expected():
    connection = _QualityConnection([[(10, 10, 9, 1, 7, 7)], [(2, 1)]])

    # affected_only: the caller's scope is used, so only the two checks run.
    with pytest.raises(
        DataQualityError,
        match=(
            "raw_rows=10, staging_rows=10, valid_rows=9, invalid_rows=1, "
            "expected_mart_rows=7, actual_mart_rows=7, "
            "winner_difference_rows=2, latest_rule_violations=1"
        ),
    ):
        assert_layer_reconciliation(  # type: ignore[arg-type]
            connection, affected_only=True
        )


def test_refresh_mart_rejects_unknown_refresh_mode_before_writes():
    connection = _RefreshConnection()

    with pytest.raises(ValueError, match="unsupported refresh mode"):
        refresh_mart(connection, refresh_mode="incremental")  # type: ignore[arg-type]

    assert connection.queries == []


def test_run_transform_rejects_rebuild_combined_with_full_refresh():
    with pytest.raises(ValueError, match="rebuild already includes"):
        run_transform(rebuild=True, full_refresh=True)


class _RawStream:
    def __init__(self, rows):
        self.rows = rows
        self.itersize = None
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query):
        self.query = query
        return self

    def __iter__(self):
        return iter(self.rows)


class _BatchWriter:
    def __init__(self):
        self.batch_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def executemany(self, _query, rows):
        self.batch_sizes.append(len(rows))


class _StreamingStageConnection:
    def __init__(self, rows):
        self.raw_stream = _RawStream(rows)
        self.batch_writer = _BatchWriter()
        self.server_cursor_name = None

    def cursor(self, *, name=None):
        if name is not None:
            self.server_cursor_name = name
            return self.raw_stream
        return self.batch_writer


def test_stage_pending_records_streams_fixed_size_batches():
    payload = {"code": "00123", "last_modified_t": 1_789_000_000}
    rows = [
        ("delta.json.gz", line_number, LOADED_AT, payload)
        for line_number in range(1, 206)
    ]
    connection = _StreamingStageConnection(rows)

    staged_count = stage_pending_records(connection)  # type: ignore[arg-type]

    assert staged_count == 205
    assert connection.server_cursor_name == "pending_raw_records"
    assert connection.raw_stream.itersize == STREAM_BATCH_SIZE
    assert connection.batch_writer.batch_sizes == [100, 100, 5]


@pytest.mark.parametrize(
    ("stage", "query", "cursor_name"),
    [
        (stage_pending_records, PENDING_RAW_RECORDS_SQL, "pending_raw_records"),
        (stage_all_records, ALL_RAW_RECORDS_SQL, "all_raw_records"),
    ],
)
def test_each_staging_entry_point_reads_its_own_raw_query(stage, query, cursor_name):
    connection = _StreamingStageConnection([])

    stage(connection)  # type: ignore[arg-type]

    assert connection.raw_stream.query == query
    assert connection.server_cursor_name == cursor_name
