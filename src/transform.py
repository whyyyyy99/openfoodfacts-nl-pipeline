"""Transform raw product records and refresh the latest-state mart."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple

from psycopg import Connection

from src.db import get_connection, load_sql


INVALID_LAST_MODIFIED_T = "invalid_last_modified_t"
MISSING_PRODUCT_CODE = "missing_product_code"
EMPTY_PRODUCT_CODE = "empty_product_code"
NON_DIGIT_PRODUCT_CODE = "non_digit_product_code"
MISSING_NUTRITION_BASIS = "missing_nutrition_basis"
UNSUPPORTED_NUTRITION_BASIS = "unsupported_nutrition_basis"
UNEXPECTED_NUTRITION_UNIT = "unexpected_nutrition_unit"
NUTRIENT_EXCEEDS_100G = "nutrient_exceeds_100g"
SUGARS_EXCEED_CARBOHYDRATES = "sugars_exceed_carbohydrates"
SATURATED_FAT_EXCEEDS_FAT = "saturated_fat_exceeds_fat"
HEURISTIC_MACRONUTRIENT_SUM_OVER_110G = (
    "heuristic_macronutrient_sum_over_110g"
)
HEURISTIC_ENERGY_OVER_950KCAL = "heuristic_energy_over_950kcal"
UNEXPECTED_NUTRISCORE_VALUE = "unexpected_nutriscore_value"

# Labels round values, often to whole grams, so near-equal pairs are not flagged.
NUTRIENT_COMPARISON_TOLERANCE_G = Decimal("1")
# Some labels count fibre inside carbohydrates, so sums slightly above 100 g are plausible.
MACRONUTRIENT_SUM_WARNING_G = Decimal("110")
ENERGY_WARNING_KCAL = Decimal("950")

NUTRIENT_UNITS = {
    "energy-kcal": "kcal",
    "energy-kj": "kJ",
    "fat": "g",
    "proteins": "g",
    "carbohydrates": "g",
    "sugars": "g",
    "salt": "g",
    "saturated-fat": "g",
}

BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
STREAM_BATCH_SIZE = 100


class DataQualityError(RuntimeError):
    """Raised when staged data violates an assumption required by the mart."""


class StagingRow(NamedTuple):
    source_file_name: str
    source_line_number: int
    loaded_at: datetime
    product_code: str | None
    created_at: datetime | None
    last_modified_at: datetime | None
    rev: int | None
    product_name: str | None
    brands_tags: list[str] | None
    categories_tags: list[str] | None
    countries_tags: list[str] | None
    nutriscore_grade: str | None
    nutriscore_status: str
    nova_group: int | None
    nutrition_basis: str | None
    energy_kcal: Decimal | None
    energy_kj: Decimal | None
    fat_g: Decimal | None
    proteins_g: Decimal | None
    carbohydrates_g: Decimal | None
    sugars_g: Decimal | None
    salt_g: Decimal | None
    saturated_fat_g: Decimal | None
    validation_errors: list[str]
    quality_warnings: list[str]


def _bigint(value: object) -> int | None:
    if type(value) is not int or not BIGINT_MIN <= value <= BIGINT_MAX:
        return None
    return value


def _integer(value: object) -> int | None:
    if type(value) is not int or not -(2**31) <= value <= 2**31 - 1:
        return None
    return value


def _epoch_timestamp(value: object) -> datetime | None:
    epoch_seconds = _bigint(value)
    if epoch_seconds is None:
        return None
    try:
        return datetime.fromtimestamp(epoch_seconds, tz=UTC)
    except (OSError, OverflowError, ValueError):
        return None


def _non_empty_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _text_array(value: object) -> list[str] | None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return None
    return value


def _numeric(value: object) -> Decimal | None:
    if type(value) not in (int, float):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _product_code(payload: Mapping[str, Any]) -> tuple[str | None, str | None]:
    if "code" not in payload or payload["code"] is None:
        return None, MISSING_PRODUCT_CODE
    if not isinstance(payload["code"], str):
        return None, NON_DIGIT_PRODUCT_CODE

    code = payload["code"].strip()
    if not code:
        return None, EMPTY_PRODUCT_CODE
    if not code.isascii() or not code.isdigit():
        return code, NON_DIGIT_PRODUCT_CODE
    return code, None


def _nutriscore(payload: Mapping[str, Any]) -> tuple[str | None, str, list[str]]:
    value = payload.get("nutriscore_grade")
    if value is None:
        return None, "missing", []
    if not isinstance(value, str):
        return None, "unexpected", [UNEXPECTED_NUTRISCORE_VALUE]

    if value in {"a", "b", "c", "d", "e"}:
        return value, "available", []
    if value == "unknown":
        return None, "unknown", []
    if value == "not-applicable":
        return None, "not_applicable", []
    return None, "unexpected", [UNEXPECTED_NUTRISCORE_VALUE]


def _append_nutrition_consistency_warnings(
    basis: str,
    values: Mapping[str, Decimal | None],
    warnings: list[str],
) -> None:
    sugars = values["sugars"]
    carbohydrates = values["carbohydrates"]
    if (
        sugars is not None
        and carbohydrates is not None
        and sugars > carbohydrates + NUTRIENT_COMPARISON_TOLERANCE_G
    ):
        warnings.append(SUGARS_EXCEED_CARBOHYDRATES)

    saturated_fat = values["saturated-fat"]
    fat = values["fat"]
    if (
        saturated_fat is not None
        and fat is not None
        and saturated_fat > fat + NUTRIENT_COMPARISON_TOLERANCE_G
    ):
        warnings.append(SATURATED_FAT_EXCEEDS_FAT)

    if basis == "100g":
        gram_values = (
            values["fat"],
            values["proteins"],
            values["carbohydrates"],
            values["sugars"],
            values["salt"],
            values["saturated-fat"],
        )
        if any(value is not None and value > 100 for value in gram_values):
            warnings.append(NUTRIENT_EXCEEDS_100G)

        macronutrient_sum = sum(
            (
                values[name] or Decimal(0)
                for name in ("fat", "proteins", "carbohydrates", "salt")
            ),
            Decimal(0),
        )
        if macronutrient_sum > MACRONUTRIENT_SUM_WARNING_G:
            warnings.append(HEURISTIC_MACRONUTRIENT_SUM_OVER_110G)

        energy_kcal = values["energy-kcal"]
        if energy_kcal is not None and energy_kcal > ENERGY_WARNING_KCAL:
            warnings.append(HEURISTIC_ENERGY_OVER_950KCAL)


def _nutrition(
    payload: Mapping[str, Any],
) -> tuple[str | None, dict[str, Decimal | None], list[str]]:
    values: dict[str, Decimal | None] = dict.fromkeys(NUTRIENT_UNITS)
    nutrition = payload.get("nutrition")
    if not isinstance(nutrition, Mapping):
        return None, values, []
    aggregated = nutrition.get("aggregated_set")
    if not isinstance(aggregated, Mapping):
        return None, values, []

    nutrients = aggregated.get("nutrients")
    has_nutrients = isinstance(nutrients, Mapping) and bool(nutrients)
    basis = aggregated.get("per")
    if basis not in ("100g", "100ml"):
        if basis is None:
            warning = [MISSING_NUTRITION_BASIS] if has_nutrients else []
        else:
            warning = [UNSUPPORTED_NUTRITION_BASIS]
        return None, values, warning

    warnings: list[str] = []
    if isinstance(nutrients, Mapping):
        for nutrient_name, expected_unit in NUTRIENT_UNITS.items():
            nutrient = nutrients.get(nutrient_name)
            if isinstance(nutrient, Mapping):
                value = _numeric(nutrient.get("value"))
                if value is None:
                    continue
                if nutrient.get("unit") != expected_unit:
                    if UNEXPECTED_NUTRITION_UNIT not in warnings:
                        warnings.append(UNEXPECTED_NUTRITION_UNIT)
                    continue
                values[nutrient_name] = value

    _append_nutrition_consistency_warnings(basis, values, warnings)
    return basis, values, warnings


def transform_payload(
    source_file_name: str,
    source_line_number: int,
    loaded_at: datetime,
    raw_payload: object,
) -> StagingRow:
    """Convert one JSON object into a staging row without raising on bad fields."""
    payload: Mapping[str, Any] = raw_payload if isinstance(raw_payload, Mapping) else {}
    validation_errors: list[str] = []

    product_code, code_error = _product_code(payload)
    if code_error:
        validation_errors.append(code_error)

    last_modified_at = _epoch_timestamp(payload.get("last_modified_t"))
    if last_modified_at is None:
        validation_errors.append(INVALID_LAST_MODIFIED_T)

    nutrition_basis, nutrients, quality_warnings = _nutrition(payload)
    nutriscore_grade, nutriscore_status, nutriscore_warnings = _nutriscore(payload)
    quality_warnings.extend(nutriscore_warnings)

    return StagingRow(
        source_file_name,
        source_line_number,
        loaded_at,
        product_code,
        _epoch_timestamp(payload.get("created_t")),
        last_modified_at,
        _bigint(payload.get("rev")),
        _non_empty_text(payload.get("product_name")),
        _text_array(payload.get("brands_tags")),
        _text_array(payload.get("categories_tags")),
        _text_array(payload.get("countries_tags")),
        nutriscore_grade,
        nutriscore_status,
        _integer(payload.get("nova_group")),
        nutrition_basis,
        nutrients["energy-kcal"],
        nutrients["energy-kj"],
        nutrients["fat"],
        nutrients["proteins"],
        nutrients["carbohydrates"],
        nutrients["sugars"],
        nutrients["salt"],
        nutrients["saturated-fat"],
        validation_errors,
        quality_warnings,
    )


STAGING_INSERT_SQL = load_sql("transform/staging/insert_record.sql")
PENDING_RAW_RECORDS_SQL = load_sql("transform/staging/select_pending_raw_records.sql")
ALL_RAW_RECORDS_SQL = load_sql("transform/staging/select_all_raw_records.sql")
CLEAR_STAGING_SQL = load_sql("transform/staging/delete_all_records.sql")


def _stage_records(connection: Connection, query: str, cursor_name: str) -> int:
    staged_count = 0
    staged_batch: list[StagingRow] = []
    with (
        connection.cursor(name=cursor_name) as raw_cursor,
        connection.cursor() as staging_cursor,
    ):
        raw_cursor.itersize = STREAM_BATCH_SIZE
        raw_cursor.execute(query)
        for raw_row in raw_cursor:
            staged_batch.append(transform_payload(*raw_row))
            if len(staged_batch) == STREAM_BATCH_SIZE:
                staging_cursor.executemany(STAGING_INSERT_SQL, staged_batch)
                staged_count += len(staged_batch)
                staged_batch.clear()
        if staged_batch:
            staging_cursor.executemany(STAGING_INSERT_SQL, staged_batch)
            staged_count += len(staged_batch)
    return staged_count


def stage_pending_records(connection: Connection) -> int:
    """Stage pending raw rows with a bounded client-side working set."""
    return _stage_records(connection, PENDING_RAW_RECORDS_SQL, "pending_raw_records")


def stage_all_records(connection: Connection) -> int:
    """Stage every raw row for an explicit full rebuild."""
    return _stage_records(connection, ALL_RAW_RECORDS_SQL, "all_raw_records")


MART_INCREMENTAL = "mart_incremental"
MART_FULL_REFRESH = "mart_full_refresh"
STAGING_AND_MART_REBUILD = "staging_and_mart_rebuild"
REFRESH_MODES = frozenset(
    {MART_INCREMENTAL, MART_FULL_REFRESH, STAGING_AND_MART_REBUILD}
)

# Wait at most this long for table locks, so a session holding a conflicting
# lock produces a clear, retryable failure instead of an indefinite hang.
LOCK_TIMEOUT = "5min"

SET_LOCK_TIMEOUT_SQL = load_sql("transform/set_lock_timeout.sql")
MART_REFRESH_LOCK_SQL = load_sql("transform/lock_for_refresh.sql")
FULL_REBUILD_LOCK_SQL = load_sql("transform/lock_for_rebuild.sql")

# The refresh scope lives in a transaction-local temporary table. Incremental
# refreshes fill it with codes that have rows not yet applied to the mart; full
# refreshes fill it with every code. All mart SQL below is scoped by it, so the
# full refresh is the incremental algorithm applied to the complete key set.
DROP_AFFECTED_CODES_SQL = load_sql("transform/mart/drop_affected_codes.sql")
CREATE_AFFECTED_CODES_SQL = load_sql("transform/mart/create_affected_codes.sql")
PENDING_AFFECTED_CODES_SQL = load_sql("transform/mart/fill_affected_codes_pending.sql")
ALL_AFFECTED_CODES_SQL = load_sql("transform/mart/fill_affected_codes_all.sql")
ANALYZE_AFFECTED_CODES_SQL = load_sql("transform/mart/analyze_affected_codes.sql")
COUNT_AFFECTED_CODES_SQL = load_sql("transform/mart/count_affected_codes.sql")

WITHIN_FILE_DUPLICATES_SQL = load_sql("transform/mart/check_within_file_duplicates.sql")
REPEATED_VERSION_KEYS_SQL = load_sql("transform/mart/check_repeated_version_keys.sql")
MART_DELETE_AFFECTED_SQL = load_sql("transform/mart/delete_affected_products.sql")
MART_INSERT_SQL = load_sql("transform/mart/insert_affected_winners.sql")
MARK_MART_APPLIED_SQL = load_sql("transform/mart/mark_staging_applied.sql")
LAYER_COUNTS_SQL = load_sql("transform/mart/check_layer_counts.sql")
WINNER_CHECK_SQL = load_sql("transform/mart/check_affected_winners.sql")
REFRESH_LOG_INSERT_SQL = load_sql("transform/mart/insert_refresh_log.sql")
CLEAR_MART_SQL = load_sql("transform/mart/delete_all_products.sql")


def _fill_affected_codes(connection: Connection, *, all_codes: bool) -> int:
    """(Re)create the refresh scope and return how many codes it holds."""
    connection.execute(DROP_AFFECTED_CODES_SQL)
    connection.execute(CREATE_AFFECTED_CODES_SQL)
    connection.execute(
        ALL_AFFECTED_CODES_SQL if all_codes else PENDING_AFFECTED_CODES_SQL
    )
    connection.execute(ANALYZE_AFFECTED_CODES_SQL)
    return connection.execute(COUNT_AFFECTED_CODES_SQL).fetchone()[0]


def assert_staging_quality(connection: Connection) -> None:
    """Fail closed on unobserved duplicate conditions within the refresh scope.

    Requires the transaction-local ``affected_codes`` table. Each affected code
    is checked against its complete staging history, not only its new rows.
    """
    within_file = connection.execute(WITHIN_FILE_DUPLICATES_SQL).fetchall()
    repeated_versions = connection.execute(REPEATED_VERSION_KEYS_SQL).fetchall()
    failures: list[str] = []
    if within_file:
        failures.append(f"within-file duplicate product codes: {within_file!r}")
    if repeated_versions:
        failures.append(f"repeated product version keys: {repeated_versions!r}")
    if failures:
        raise DataQualityError("; ".join(failures))


def assert_layer_reconciliation(
    connection: Connection, *, affected_only: bool = False
) -> None:
    """Require layer correspondence and exact, latest mart winners.

    Row-count invariants are always global. Winner rows are compared on every
    mart column, plus an independently formulated latest-timestamp rule, for
    the codes in ``affected_codes``. With ``affected_only`` the caller's scope
    is used as is; otherwise the scope is first filled with every code.
    """
    if not affected_only:
        _fill_affected_codes(connection, all_codes=True)
    (
        raw_rows,
        staging_rows,
        valid_rows,
        invalid_rows,
        expected_mart_rows,
        actual_mart_rows,
    ) = connection.execute(LAYER_COUNTS_SQL).fetchone()
    winner_difference_rows, latest_rule_violations = connection.execute(
        WINNER_CHECK_SQL
    ).fetchone()
    if (
        raw_rows != staging_rows
        or staging_rows != valid_rows + invalid_rows
        or expected_mart_rows != actual_mart_rows
        or winner_difference_rows != 0
        or latest_rule_violations != 0
    ):
        raise DataQualityError(
            "layer reconciliation failed: "
            f"raw_rows={raw_rows}, staging_rows={staging_rows}, "
            f"valid_rows={valid_rows}, invalid_rows={invalid_rows}, "
            f"expected_mart_rows={expected_mart_rows}, "
            f"actual_mart_rows={actual_mart_rows}, "
            f"winner_difference_rows={winner_difference_rows}, "
            f"latest_rule_violations={latest_rule_violations}"
        )


def refresh_mart(
    connection: Connection, *, refresh_mode: str = MART_INCREMENTAL
) -> int:
    """Refresh mart winners for affected codes and return the mart row count.

    ``mart_incremental`` recomputes only codes with staging rows not yet applied
    to the mart; the full modes recompute every code. Each affected code is
    recomputed from its complete staging history, so arrival order does not
    matter. Scope, quality checks, rewrite, reconciliation, the applied marker
    and the refresh log all commit in the caller's transaction or not at all.
    """
    if refresh_mode not in REFRESH_MODES:
        raise ValueError(f"unsupported refresh mode: {refresh_mode!r}")
    connection.execute(SET_LOCK_TIMEOUT_SQL, (LOCK_TIMEOUT,))
    connection.execute(MART_REFRESH_LOCK_SQL)
    affected_count = _fill_affected_codes(
        connection, all_codes=refresh_mode != MART_INCREMENTAL
    )

    assert_staging_quality(connection)
    connection.execute(MART_DELETE_AFFECTED_SQL)
    connection.execute(MART_INSERT_SQL)
    # The scope already holds every code in the full modes.
    assert_layer_reconciliation(connection, affected_only=True)
    connection.execute(MARK_MART_APPLIED_SQL)
    mart_row_count = connection.execute(
        REFRESH_LOG_INSERT_SQL, (refresh_mode, affected_count)
    ).fetchone()[0]
    connection.execute(DROP_AFFECTED_CODES_SQL)
    return mart_row_count


def rebuild_staging_and_mart(connection: Connection) -> tuple[int, int]:
    """Recompute staging and mart atomically from the complete raw record set."""
    with connection.transaction():
        connection.execute(SET_LOCK_TIMEOUT_SQL, (LOCK_TIMEOUT,))
        connection.execute(FULL_REBUILD_LOCK_SQL)
        connection.execute(CLEAR_MART_SQL)
        connection.execute(CLEAR_STAGING_SQL)
        staged_count = stage_all_records(connection)
        mart_count = refresh_mart(connection, refresh_mode=STAGING_AND_MART_REBUILD)
    return staged_count, mart_count


def run_transform(
    *, rebuild: bool = False, full_refresh: bool = False
) -> tuple[int, int]:
    """Stage raw records and refresh mart; return staged rows and mart rows.

    The default refreshes only affected mart codes. ``full_refresh`` recomputes
    every mart code from existing staging (for mart-logic changes or a periodic
    full check); ``rebuild`` also recomputes staging from raw.
    """
    if rebuild and full_refresh:
        raise ValueError("rebuild already includes a full mart refresh")
    if rebuild:
        with get_connection() as connection:
            return rebuild_staging_and_mart(connection)

    with get_connection() as connection:
        staged_count = stage_pending_records(connection)
    with get_connection() as connection:
        mart_count = refresh_mart(
            connection,
            refresh_mode=MART_FULL_REFRESH if full_refresh else MART_INCREMENTAL,
        )
    return staged_count, mart_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--full-refresh",
        action="store_true",
        help="recompute every mart code from existing staging",
    )
    mode.add_argument(
        "--rebuild",
        action="store_true",
        help="atomically rebuild staging and mart from all committed raw rows",
    )
    args = parser.parse_args()
    staged, mart = run_transform(rebuild=args.rebuild, full_refresh=args.full_refresh)
    print(f"staged rows: {staged}")
    print(f"mart rows: {mart}")
