#!/usr/bin/env python3
"""Profile product-code completeness, format, lengths, and normalisation collisions."""

from __future__ import annotations

import argparse
import collections
import re
from pathlib import Path
from typing import Any

from _common import iter_lines, ordered_windows, ratio, type_name


def collision_groups(values: set[str], normalise: Any) -> list[list[str]]:
    groups: dict[str, set[str]] = collections.defaultdict(set)
    for value in values:
        groups[normalise(value)].add(value)
    return sorted([sorted(group) for group in groups.values() if len(group) > 1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    counts = collections.Counter()
    types = collections.Counter()
    lengths = collections.Counter()
    raw_strings: set[str] = set()

    for item in iter_lines(windows):
        if item.error or not isinstance(item.value, dict):
            continue
        counts["records"] += 1
        record = item.value
        if "code" not in record:
            counts["missing_key"] += 1
            continue
        value = record["code"]
        types[type_name(value)] += 1
        if value is None:
            counts["null"] += 1
        if not isinstance(value, str):
            counts["non_string"] += 1
            continue
        raw_strings.add(value)
        if value == "":
            counts["empty_string"] += 1
        if value != "" and not value.strip():
            counts["whitespace_only"] += 1
        if value != value.strip():
            counts["surrounding_whitespace"] += 1
        if value and re.fullmatch(r"[0-9]+", value) is None:
            counts["non_ascii_digit"] += 1
        if value:
            lengths[len(value)] += 1
        if value.startswith("0"):
            counts["leading_zero"] += 1

    total = counts["records"]
    print(f"Records examined: {total:,}")
    for name in (
        "missing_key",
        "null",
        "empty_string",
        "whitespace_only",
        "surrounding_whitespace",
        "non_string",
        "non_ascii_digit",
        "leading_zero",
    ):
        print(f"{name}: {ratio(counts[name], total)}")
    print("Observed code JSON types:")
    for name, count in sorted(types.items()):
        print(f"  {name}: {ratio(count, total)}")
    print("Length distribution among non-empty string codes:")
    for length, count in sorted(lengths.items()):
        print(f"  length={length}: {count:,}")

    whitespace = collision_groups(raw_strings, lambda value: value.strip())
    leading_zero = collision_groups(
        {value for value in raw_strings if re.fullmatch(r"[0-9]+", value)},
        lambda value: value.lstrip("0") or "0",
    )
    print(f"Distinct raw string codes: {len(raw_strings):,}")
    print(f"Whitespace-normalisation collision groups: {len(whitespace):,}")
    for group in whitespace[:10]:
        print(f"  {group}")
    print(f"Leading-zero-normalisation collision groups: {len(leading_zero):,}")
    for group in leading_zero[:10]:
        print(f"  {group}")


if __name__ == "__main__":
    main()
