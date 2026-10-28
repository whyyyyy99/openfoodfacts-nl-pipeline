#!/usr/bin/env python3
"""Profile version fields, filename-window fit, ordering, and timestamp ties."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _common import iter_lines, numeric, ordered_windows, ratio, type_name


FIELDS = ("last_modified_t", "rev", "created_t")


@dataclass(frozen=True)
class Version:
    file_order: int
    line_number: int
    last_modified_t: Any
    rev: Any


def utc(value: float | None) -> str:
    if value is None:
        return "n/a"
    return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    total = 0
    present = collections.Counter()
    numeric_values: dict[str, list[float]] = {field: [] for field in FIELDS}
    types: dict[str, collections.Counter[str]] = {
        field: collections.Counter() for field in FIELDS
    }
    inside = outside = invalid_window_value = 0
    by_code: dict[str, list[Version]] = collections.defaultdict(list)

    for file_order, window in enumerate(windows):
        for item in iter_lines([window]):
            if item.error or not isinstance(item.value, dict):
                continue
            total += 1
            record = item.value
            for field in FIELDS:
                if field in record:
                    present[field] += 1
                    types[field][type_name(record[field])] += 1
                    value = numeric(record[field])
                    if value is not None:
                        numeric_values[field].append(value)
            modified = numeric(record.get("last_modified_t"))
            if modified is None:
                invalid_window_value += 1
            elif window.start <= modified <= window.end:
                inside += 1
            else:
                outside += 1
            code = record.get("code")
            if isinstance(code, str):
                by_code[code].append(
                    Version(
                        file_order=file_order,
                        line_number=item.line_number,
                        last_modified_t=record.get("last_modified_t"),
                        rev=record.get("rev"),
                    )
                )

    print(f"Records examined: {total:,}")
    for field in FIELDS:
        values = numeric_values[field]
        print(f"{field}:")
        print(f"  present: {ratio(present[field], total)}")
        print(f"  types: {dict(sorted(types[field].items()))}")
        print(f"  numeric: {ratio(len(values), total)}")
        if values:
            if field.endswith("_t"):
                print(
                    f"  range: {int(min(values))} ({utc(min(values))}) .. "
                    f"{int(max(values))} ({utc(max(values))})"
                )
            else:
                print(f"  range: {min(values):g} .. {max(values):g}")
    print(f"last_modified_t inside filename window: {ratio(inside, total)}")
    print(f"last_modified_t outside filename window: {ratio(outside, total)}")
    print(f"last_modified_t missing/non-numeric for window check: {ratio(invalid_window_value, total)}")

    repeated = {code: rows for code, rows in by_code.items() if len(rows) > 1}
    comparisons = collections.Counter()
    tie_groups: dict[tuple[str, float], list[Version]] = collections.defaultdict(list)
    for code, versions in repeated.items():
        ordered = sorted(versions, key=lambda row: (row.file_order, row.line_number))
        for older, newer in zip(ordered, ordered[1:]):
            old = numeric(older.last_modified_t)
            new = numeric(newer.last_modified_t)
            if old is None or new is None:
                comparisons["missing_or_non_numeric"] += 1
            elif new > old:
                comparisons["increases"] += 1
            elif new == old:
                comparisons["ties"] += 1
            else:
                comparisons["decreases"] += 1
        for version in versions:
            modified = numeric(version.last_modified_t)
            if modified is not None:
                tie_groups[(code, modified)].append(version)

    actual_ties = {key: rows for key, rows in tie_groups.items() if len(rows) > 1}
    rev_breaks = rev_ties = rev_unusable = 0
    for versions in actual_ties.values():
        revs = [numeric(version.rev) for version in versions]
        if any(value is None for value in revs):
            rev_unusable += 1
        elif len(set(revs)) == len(revs):
            rev_breaks += 1
        else:
            rev_ties += 1

    print(f"Codes seen more than once: {len(repeated):,}")
    print(f"Adjacent version comparisons: {sum(comparisons.values()):,}")
    for name in ("increases", "ties", "decreases", "missing_or_non_numeric"):
        print(f"  last_modified_t {name}: {comparisons[name]:,}")
    print(f"Repeated (code, last_modified_t) tie groups: {len(actual_ties):,}")
    print(f"  ties broken by distinct numeric rev values: {rev_breaks:,}")
    print(f"  ties remaining in rev: {rev_ties:,}")
    print(f"  ties with missing/non-numeric rev: {rev_unusable:,}")


if __name__ == "__main__":
    main()
