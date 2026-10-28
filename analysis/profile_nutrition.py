#!/usr/bin/env python3
"""Profile the current and legacy nutrition structures in OFF delta files."""

from __future__ import annotations

import argparse
import collections
import json
import math
from pathlib import Path
from typing import Any

from _common import iter_lines, numeric, ordered_windows, ratio, type_name


MISSING = "<missing>"


def printable(value: Any) -> str:
    """Return a stable label for values used as Counter keys."""
    if value is None:
        return "<null>"
    if isinstance(value, str):
        return value if value else "<empty string>"
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def legacy_nutrient_names(nutriments: dict[str, Any]) -> set[str]:
    """Collapse common legacy suffixes to approximate nutrient base names."""
    suffixes = ("_100g", "_serving", "_value", "_unit")
    names: set[str] = set()
    for key in nutriments:
        name = key
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        names.add(name)
    return names


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    total = 0

    nutrition_present = 0
    nutrition_types: collections.Counter[str] = collections.Counter()
    nutrition_child_presence: collections.Counter[str] = collections.Counter()
    nutrition_child_types: dict[str, collections.Counter[str]] = collections.defaultdict(
        collections.Counter
    )

    aggregated_present = 0
    aggregated_types: collections.Counter[str] = collections.Counter()
    aggregated_per: collections.Counter[str] = collections.Counter()
    nutrients_present = 0
    nutrients_types: collections.Counter[str] = collections.Counter()
    nonempty_nutrient_maps = 0

    nutrient_occurrence: collections.Counter[str] = collections.Counter()
    nutrient_entry_types: dict[str, collections.Counter[str]] = collections.defaultdict(
        collections.Counter
    )
    nutrient_units: dict[str, collections.Counter[str]] = collections.defaultdict(
        collections.Counter
    )
    nutrient_source_per: dict[str, collections.Counter[str]] = collections.defaultdict(
        collections.Counter
    )
    entry_field_presence: collections.Counter[str] = collections.Counter()
    entry_field_types: dict[str, collections.Counter[str]] = collections.defaultdict(
        collections.Counter
    )
    entry_shapes: collections.Counter[tuple[str, ...]] = collections.Counter()
    modifier_values: collections.Counter[str] = collections.Counter()
    nutrient_entries = 0
    energy_pair: collections.Counter[str] = collections.Counter()
    energy_unit_pair: collections.Counter[str] = collections.Counter()
    salt_sodium_pair: collections.Counter[str] = collections.Counter()

    input_sets_present = 0
    input_sets_types: collections.Counter[str] = collections.Counter()
    input_set_row_lengths: collections.Counter[int] = collections.Counter()
    input_set_count = 0
    input_set_per: collections.Counter[str] = collections.Counter()
    input_set_preparation: collections.Counter[str] = collections.Counter()
    input_set_source: collections.Counter[str] = collections.Counter()

    legacy_present = 0
    legacy_nonempty = 0
    current_nonempty = 0
    both_keys_present = 0
    both_nonempty = 0
    coexistence_examples: list[dict[str, Any]] = []

    for item in iter_lines(windows):
        if item.error or not isinstance(item.value, dict):
            continue
        total += 1
        record = item.value

        has_legacy = "nutriments" in record
        legacy = record.get("nutriments")
        legacy_is_nonempty = isinstance(legacy, dict) and bool(legacy)
        legacy_present += has_legacy
        legacy_nonempty += legacy_is_nonempty

        has_nutrition = "nutrition" in record
        nutrition = record.get("nutrition")
        nutrition_present += has_nutrition
        if has_nutrition:
            nutrition_types[type_name(nutrition)] += 1

        if isinstance(nutrition, dict):
            for key, value in nutrition.items():
                nutrition_child_presence[key] += 1
                nutrition_child_types[key][type_name(value)] += 1

        aggregated = nutrition.get("aggregated_set") if isinstance(nutrition, dict) else None
        if isinstance(nutrition, dict) and "aggregated_set" in nutrition:
            aggregated_present += 1
            aggregated_types[type_name(aggregated)] += 1

        if isinstance(aggregated, dict):
            aggregated_per[printable(aggregated.get("per", MISSING))] += 1
            nutrients = aggregated.get("nutrients")
            if "nutrients" in aggregated:
                nutrients_present += 1
                nutrients_types[type_name(nutrients)] += 1
        else:
            nutrients = None

        current_is_nonempty = isinstance(nutrients, dict) and bool(nutrients)
        current_nonempty += current_is_nonempty
        nonempty_nutrient_maps += current_is_nonempty

        if isinstance(nutrients, dict):
            for nutrient, entry in nutrients.items():
                nutrient_occurrence[nutrient] += 1
                nutrient_entry_types[nutrient][type_name(entry)] += 1
                nutrient_entries += 1
                if not isinstance(entry, dict):
                    nutrient_units[nutrient][MISSING] += 1
                    nutrient_source_per[nutrient][MISSING] += 1
                    continue

                entry_shapes[tuple(sorted(entry))] += 1
                for field, value in entry.items():
                    entry_field_presence[field] += 1
                    entry_field_types[field][type_name(value)] += 1
                if "modifier" in entry:
                    modifier_values[printable(entry["modifier"])] += 1
                nutrient_units[nutrient][
                    printable(entry.get("unit", MISSING))
                ] += 1
                nutrient_source_per[nutrient][
                    printable(entry.get("source_per", MISSING))
                ] += 1

            energy = nutrients.get("energy")
            energy_kj = nutrients.get("energy-kj")
            energy_kcal = nutrients.get("energy-kcal")
            if isinstance(energy, dict) and isinstance(energy_kj, dict):
                energy_pair["both_present"] += 1
                left = numeric(energy.get("value"))
                right = numeric(energy_kj.get("value"))
                if left is None or right is None:
                    energy_pair["non_numeric"] += 1
                elif math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12):
                    energy_pair["equal_values"] += 1
                else:
                    energy_pair["different_values"] += 1

            energy_kj_value = (
                numeric(energy_kj.get("value"))
                if isinstance(energy_kj, dict)
                else None
            )
            energy_kcal_value = (
                numeric(energy_kcal.get("value"))
                if isinstance(energy_kcal, dict)
                else None
            )
            if energy_kj_value is not None and energy_kcal_value is not None:
                energy_unit_pair["both_numeric"] += 1
                difference = abs(energy_kj_value - energy_kcal_value * 4.184)
                tolerance = max(1.0, abs(energy_kj_value) * 0.01)
                if difference <= tolerance:
                    energy_unit_pair["within_1pct_or_1kj"] += 1
                else:
                    energy_unit_pair["outside_tolerance"] += 1
            elif energy_kj_value is not None:
                energy_unit_pair["kj_only"] += 1
            elif energy_kcal_value is not None:
                energy_unit_pair["kcal_only"] += 1
            else:
                energy_unit_pair["neither_numeric"] += 1

            salt = nutrients.get("salt")
            sodium = nutrients.get("sodium")
            if isinstance(salt, dict) and isinstance(sodium, dict):
                salt_sodium_pair["both_present"] += 1
                salt_value = numeric(salt.get("value"))
                sodium_value = numeric(sodium.get("value"))
                if salt_value is None or sodium_value is None:
                    salt_sodium_pair["non_numeric"] += 1
                elif math.isclose(
                    salt_value, sodium_value * 2.5, rel_tol=1e-12, abs_tol=1e-12
                ):
                    salt_sodium_pair["salt_equals_2_5_x_sodium"] += 1
                else:
                    salt_sodium_pair["different_relationship"] += 1

        input_sets = nutrition.get("input_sets") if isinstance(nutrition, dict) else None
        if isinstance(nutrition, dict) and "input_sets" in nutrition:
            input_sets_present += 1
            input_sets_types[type_name(input_sets)] += 1
        if isinstance(input_sets, list):
            input_set_row_lengths[len(input_sets)] += 1
            for input_set in input_sets:
                input_set_count += 1
                if not isinstance(input_set, dict):
                    continue
                input_set_per[printable(input_set.get("per", MISSING))] += 1
                input_set_preparation[
                    printable(input_set.get("preparation", MISSING))
                ] += 1
                input_set_source[printable(input_set.get("source", MISSING))] += 1

        both_keys_present += has_legacy and has_nutrition
        both_nonempty += legacy_is_nonempty and current_is_nonempty
        if legacy_is_nonempty and current_is_nonempty and len(coexistence_examples) < 10:
            legacy_names = legacy_nutrient_names(legacy)
            current_names = set(nutrients)
            coexistence_examples.append(
                {
                    "source_file_name": item.window.path.name,
                    "source_line_number": item.line_number,
                    "code": record.get("code"),
                    "aggregated_per": aggregated.get("per"),
                    "legacy_nutrient_names": sorted(legacy_names),
                    "current_nutrient_names": sorted(current_names),
                    "overlapping_nutrient_names": sorted(legacy_names & current_names),
                    "legacy_nutriments": legacy,
                    "current_nutrients": nutrients,
                }
            )

    print(f"Records examined: {total:,}")
    print("Current nutrition container:")
    print(f"  nutrition key present: {ratio(nutrition_present, total)}")
    print(f"  nutrition types: {dict(sorted(nutrition_types.items()))}")
    print(f"  aggregated_set key present: {ratio(aggregated_present, total)}")
    print(f"  aggregated_set types: {dict(sorted(aggregated_types.items()))}")
    print(f"  aggregated_set.nutrients key present: {ratio(nutrients_present, total)}")
    print(f"  aggregated_set.nutrients types: {dict(sorted(nutrients_types.items()))}")
    print(f"  non-empty aggregated nutrient maps: {ratio(nonempty_nutrient_maps, total)}")

    print("aggregated_set.per distribution (all records as denominator):")
    for value in ("100g", "100ml"):
        print(f"  {value}: {ratio(aggregated_per[value], total)}")
    aggregated_absent = total - aggregated_present
    per_missing_with_aggregated = aggregated_per[MISSING]
    missing_per = aggregated_absent + per_missing_with_aggregated
    print(f"  missing: {ratio(missing_per, total)}")
    print(f"    aggregated_set absent: {ratio(aggregated_absent, total)}")
    print(
        "    per missing inside aggregated_set: "
        f"{ratio(per_missing_with_aggregated, total)}"
    )
    other_per = {
        value: count
        for value, count in sorted(aggregated_per.items())
        if value not in {"100g", "100ml", MISSING}
    }
    print(f"  other: {ratio(sum(other_per.values()), total)} values={other_per}")

    print("Keys directly inside nutrition:")
    for key in sorted(nutrition_child_presence):
        print(
            f"  {key}: {ratio(nutrition_child_presence[key], total)} "
            f"types={dict(sorted(nutrition_child_types[key].items()))}"
        )

    print("input_sets summary:")
    print(f"  key present: {ratio(input_sets_present, total)}")
    print(f"  types: {dict(sorted(input_sets_types.items()))}")
    print(f"  total individual sets: {input_set_count:,}")
    print(f"  sets per record: {dict(sorted(input_set_row_lengths.items()))}")
    records_with_multiple_input_sets = sum(
        count for length, count in input_set_row_lengths.items() if length > 1
    )
    print(
        "  records with multiple input sets: "
        f"{ratio(records_with_multiple_input_sets, total)}"
    )
    print(f"  per: {dict(sorted(input_set_per.items()))}")
    print(f"  preparation: {dict(sorted(input_set_preparation.items()))}")
    print(f"  source: {dict(sorted(input_set_source.items()))}")

    print("Nutrient entry structure across all aggregated nutrients:")
    print(f"  total nutrient entries: {nutrient_entries:,}")
    all_entry_types = collections.Counter()
    for counts in nutrient_entry_types.values():
        all_entry_types.update(counts)
    print(f"  entry JSON types: {dict(sorted(all_entry_types.items()))}")
    print("  entry subfields:")
    for field in sorted(entry_field_presence):
        print(
            f"    {field}: {ratio(entry_field_presence[field], nutrient_entries)} "
            f"types={dict(sorted(entry_field_types[field].items()))}"
        )
    print("  entry object shapes:")
    for shape, count in sorted(entry_shapes.items(), key=lambda pair: (-pair[1], pair[0])):
        print(f"    {count:,}: {', '.join(shape)}")
    print(f"  modifier values: {dict(sorted(modifier_values.items()))}")

    print("Potentially redundant nutrient values:")
    print(f"  energy versus energy-kj: {dict(sorted(energy_pair.items()))}")
    print(
        "  energy-kcal versus energy-kj conversion: "
        f"{dict(sorted(energy_unit_pair.items()))}"
    )
    print(f"  salt versus sodium: {dict(sorted(salt_sodium_pair.items()))}")

    print("Nutrient keys, occurrence rates, entry types, units, and source bases:")
    print(f"  distinct nutrient keys: {len(nutrient_occurrence):,}")
    mixed_unit_nutrients = {
        nutrient: dict(sorted(units.items()))
        for nutrient, units in sorted(nutrient_units.items())
        if len(units) > 1
    }
    print(
        "  nutrient keys with more than one observed unit: "
        f"{len(mixed_unit_nutrients):,} {mixed_unit_nutrients}"
    )
    for nutrient in sorted(
        nutrient_occurrence, key=lambda key: (-nutrient_occurrence[key], key)
    ):
        count = nutrient_occurrence[nutrient]
        print(f"  {nutrient}:")
        print(f"    all records: {ratio(count, total)}")
        print(f"    non-empty nutrient maps: {ratio(count, nonempty_nutrient_maps)}")
        print(f"    entry types: {dict(sorted(nutrient_entry_types[nutrient].items()))}")
        print(f"    units: {dict(sorted(nutrient_units[nutrient].items()))}")
        print(f"    source_per: {dict(sorted(nutrient_source_per[nutrient].items()))}")

    print("Legacy nutriments versus current aggregated nutrients:")
    print(f"  legacy key present: {ratio(legacy_present, total)}")
    print(f"  legacy non-empty object: {ratio(legacy_nonempty, total)}")
    print(f"  current non-empty nutrient map: {ratio(current_nonempty, total)}")
    print(f"  both top-level keys present: {ratio(both_keys_present, total)}")
    print(f"  both representations non-empty: {ratio(both_nonempty, total)}")
    print("  non-empty coexistence examples:")
    for example in coexistence_examples:
        print("    " + json.dumps(example, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
