#!/usr/bin/env python3
"""Inspect fields and values that may describe deletion or inactive status."""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

from _common import iter_lines, ordered_windows, ratio, type_name


KEY_RE = re.compile(r"delet|obsolete|removed|withdraw|inactive|archiv|status|state", re.I)
VALUE_RE = re.compile(r"delet|obsolete|removed|withdraw|inactive|archiv|discontinu", re.I)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    total = 0
    presence = collections.Counter()
    nonempty = collections.Counter()
    types: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    values: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    matching_value_records = 0
    matching_value_examples: list[tuple[str, int, str, str]] = []

    for item in iter_lines(windows):
        if item.error or not isinstance(item.value, dict):
            continue
        total += 1
        record_has_matching_value = False
        for key, value in item.value.items():
            if KEY_RE.search(key):
                presence[key] += 1
                types[key][type_name(value)] += 1
                if value not in (None, "", [], {}):
                    nonempty[key] += 1
                rendered_value = json.dumps(value, ensure_ascii=False, sort_keys=True)
                values[key][rendered_value] += 1
                if VALUE_RE.search(rendered_value):
                    record_has_matching_value = True
                    if len(matching_value_examples) < 20:
                        matching_value_examples.append(
                            (item.window.path.name, item.line_number, key, repr(value)[:240])
                        )
        matching_value_records += record_has_matching_value

    print(f"Records examined: {total:,}")
    print(f"Status/deletion-like keys: {len(presence):,}")
    for key in sorted(presence, key=lambda name: (-presence[name], name)):
        print(
            f"  {key}: present={ratio(presence[key], total)} "
            f"nonempty={ratio(nonempty[key], presence[key])} "
            f"types={dict(sorted(types[key].items()))}"
        )
        for value, count in values[key].most_common(10):
            rendered = value if len(value) <= 240 else value[:237] + "..."
            print(f"    {count:,} x {rendered}")
    print(
        "Records with deletion/inactive-like text in a candidate status field: "
        f"{ratio(matching_value_records, total)}"
    )
    for filename, line_number, key, value in matching_value_examples:
        print(f"  example {filename}:{line_number} key={key} value={value}")


if __name__ == "__main__":
    main()
