#!/usr/bin/env python3
"""Profile top-level field occurrence, emptiness, JSON types, and name languages."""

from __future__ import annotations

import argparse
import collections
import re
from pathlib import Path
from typing import Any

from _common import iter_lines, ordered_windows, ratio, type_name


BASE_CANDIDATES = (
    "product_name",
    "brands",
    "categories_tags",
    "countries_tags",
    "nutriscore_grade",
    "nova_group",
    "nutriments",
)
LANGUAGE_NAME_RE = re.compile(r"^product_name_(?P<language>[a-z]{2,3})$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    total = 0
    presence = collections.Counter()
    nulls = collections.Counter()
    empty_strings = collections.Counter()
    empty_arrays = collections.Counter()
    empty_objects = collections.Counter()
    types: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    language_fields: set[str] = set()

    for item in iter_lines(windows):
        if item.error or not isinstance(item.value, dict):
            continue
        total += 1
        for key, value in item.value.items():
            presence[key] += 1
            types[key][type_name(value)] += 1
            nulls[key] += value is None
            empty_strings[key] += value == ""
            empty_arrays[key] += value == []
            empty_objects[key] += value == {}
            if LANGUAGE_NAME_RE.fullmatch(key):
                language_fields.add(key)

    print(f"Records examined: {total:,}")
    print(f"Distinct top-level keys: {len(presence):,}")
    ranked = sorted(presence, key=lambda key: (-presence[key], key))
    print("Top 50 keys by occurrence:")
    for key in ranked[:50]:
        print(f"  {key}: {ratio(presence[key], total)} types={dict(sorted(types[key].items()))}")
    below_half = [key for key in ranked if presence[key] * 2 < total]
    print(f"Keys below 50% occurrence: {len(below_half):,}")
    for key in below_half:
        print(f"  {key}: {ratio(presence[key], total)}")

    candidates = sorted(set(BASE_CANDIDATES) | language_fields)
    print("Candidate-field missingness and emptiness:")
    for key in candidates:
        print(
            f"  {key}: missing={ratio(total - presence[key], total)} "
            f"null={ratio(nulls[key], total)} "
            f"empty_string={ratio(empty_strings[key], total)} "
            f"empty_array={ratio(empty_arrays[key], total)} "
            f"empty_object={ratio(empty_objects[key], total)} "
            f"present={ratio(presence[key], total)} "
            f"types={dict(sorted(types[key].items()))}"
        )

    inconsistent = sorted(key for key, counts in types.items() if len(counts) > 1)
    print(f"Keys with inconsistent JSON types: {len(inconsistent):,}")
    for key in inconsistent:
        print(f"  {key}: present={presence[key]:,} types={dict(sorted(types[key].items()))}")

    suffix_counts = collections.Counter()
    for field in language_fields:
        match = LANGUAGE_NAME_RE.fullmatch(field)
        assert match is not None
        suffix_counts[match["language"]] += presence[field]
    print(f"Observed product_name language suffixes: {len(suffix_counts):,}")
    for language, count in sorted(suffix_counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {language}: field_present_in={count:,} record(s)")
    print(f"product_name present: {ratio(presence['product_name'], total)}")
    print(f"product_name missing: {ratio(total - presence['product_name'], total)}")


if __name__ == "__main__":
    main()
