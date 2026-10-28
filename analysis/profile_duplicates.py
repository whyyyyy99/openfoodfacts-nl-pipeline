#!/usr/bin/env python3
"""Profile within-file, cross-file, and repeated-version duplicates."""

from __future__ import annotations

import argparse
import collections
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _common import iter_lines, ordered_windows


@dataclass(frozen=True)
class Occurrence:
    filename: str
    line_number: int
    last_modified_t: Any
    rev: Any


def changed_fields(records: list[dict[str, Any]]) -> list[str]:
    keys = set().union(*(record.keys() for record in records))
    return sorted(
        key
        for key in keys
        if len(
            {
                json.dumps(record.get(key), sort_keys=True, ensure_ascii=False)
                for record in records
            }
        )
        > 1
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--example-limit", type=int, default=3)
    parser.add_argument("--inspect-code", help="Print version locations and changed fields for one code")
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    by_file_code = collections.Counter()
    by_code: dict[str, list[Occurrence]] = collections.defaultdict(list)
    version_key_counts = collections.Counter()

    for window in windows:
        for item in iter_lines([window]):
            if item.error or not isinstance(item.value, dict):
                continue
            code = item.value.get("code")
            if not isinstance(code, str):
                continue
            by_file_code[(window.path.name, code)] += 1
            by_code[code].append(
                Occurrence(
                    filename=window.path.name,
                    line_number=item.line_number,
                    last_modified_t=item.value.get("last_modified_t"),
                    rev=item.value.get("rev"),
                )
            )
            modified_key = json.dumps(
                item.value.get("last_modified_t"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            version_key_counts[(code, modified_key)] += 1

    within = {key: count for key, count in by_file_code.items() if count > 1}
    cross = {
        code: rows
        for code, rows in by_code.items()
        if len({row.filename for row in rows}) > 1
    }
    repeated_version_keys = {
        key for key, count in version_key_counts.items() if count > 1
    }

    print(f"Distinct string codes: {len(by_code):,}")
    print(f"Within-file duplicate code groups: {len(within):,}")
    print(f"Within-file excess rows: {sum(count - 1 for count in within.values()):,}")
    print(f"Cross-file duplicate codes: {len(cross):,}")
    print(
        "Cross-file excess file appearances: "
        f"{sum(len({row.filename for row in rows}) - 1 for rows in cross.values()):,}"
    )
    distribution = collections.Counter(len(rows) for rows in by_code.values())
    print("Code occurrence-count distribution:")
    for count, products in sorted(distribution.items()):
        print(f"  {count} occurrence(s): {products:,} code(s)")
    print(f"Repeated (code, last_modified_t) groups: {len(repeated_version_keys):,}")

    payloads: dict[tuple[str, Any], list[tuple[bytes, dict[str, Any], str, int]]] = collections.defaultdict(list)
    inspected: list[tuple[dict[str, Any], str, int]] = []
    if repeated_version_keys or args.inspect_code:
        for window in windows:
            for item in iter_lines([window]):
                if item.error or not isinstance(item.value, dict):
                    continue
                key = (
                    item.value.get("code"),
                    json.dumps(
                        item.value.get("last_modified_t"),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                )
                if key in repeated_version_keys:
                    payloads[key].append(
                        (item.raw_record, item.value, window.path.name, item.line_number)
                    )
                if item.value.get("code") == args.inspect_code:
                    inspected.append((item.value, window.path.name, item.line_number))

    byte_identical = json_identical = differing = 0
    differing_fields = collections.Counter()
    for rows in payloads.values():
        raw_values = {row[0] for row in rows}
        canonical = {
            json.dumps(row[1], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            for row in rows
        }
        if len(raw_values) == 1:
            byte_identical += 1
        if len(canonical) == 1:
            json_identical += 1
        else:
            differing += 1
            differing_fields.update(changed_fields([row[1] for row in rows]))
    print(f"  byte-identical payload groups: {byte_identical:,}")
    print(f"  JSON-equivalent payload groups: {json_identical:,}")
    print(f"  differing payload groups: {differing:,}")
    print("  fields differing within repeated version keys:")
    for field, count in differing_fields.most_common():
        print(f"    {field}: {count:,} group(s)")

    print("Cross-file examples for manual inspection:")
    for code, rows in sorted(cross.items())[: max(args.example_limit, 0)]:
        print(f"  code={code}")
        for row in rows:
            print(
                f"    {row.filename}:{row.line_number} "
                f"last_modified_t={row.last_modified_t!r} rev={row.rev!r}"
            )

    if args.inspect_code:
        print(f"Manual inspection for code={args.inspect_code}:")
        print(f"  occurrences={len(inspected):,}")
        for record, filename, line_number in inspected:
            print(
                f"  {filename}:{line_number} "
                f"last_modified_t={record.get('last_modified_t')!r} "
                f"rev={record.get('rev')!r} product_name={record.get('product_name')!r}"
            )
        for (older, _, _), (newer, _, _) in zip(inspected, inspected[1:]):
            fields = changed_fields([older, newer])
            print(f"  changed_fields ({len(fields)}): {', '.join(fields)}")


if __name__ == "__main__":
    main()
