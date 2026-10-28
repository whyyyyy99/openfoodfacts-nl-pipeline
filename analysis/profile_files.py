#!/usr/bin/env python3
"""Report file-level row counts, parse failures, windows, and index order."""

from __future__ import annotations

import argparse
from pathlib import Path

from _common import iter_lines, ordered_windows, parse_window


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--index", type=Path, help="Optional live index file")
    args = parser.parse_args()

    windows = ordered_windows(args.files)
    print("Processing order: oldest to newest")
    total_lines = total_parsed = total_failures = total_non_objects = 0
    for window in windows:
        lines = parsed = failures = non_objects = 0
        for item in iter_lines([window]):
            lines += 1
            if item.error:
                failures += 1
            elif not isinstance(item.value, dict):
                non_objects += 1
            else:
                parsed += 1
        total_lines += lines
        total_parsed += parsed
        total_failures += failures
        total_non_objects += non_objects
        print(
            f"{window.path}: lines={lines:,} parsed_objects={parsed:,} "
            f"parse_failures={failures:,} non_object_json={non_objects:,} "
            f"window=[{window.start}, {window.end}]"
        )

    print(
        f"TOTAL: lines={total_lines:,} parsed_objects={total_parsed:,} "
        f"parse_failures={total_failures:,} non_object_json={total_non_objects:,}"
    )

    print("Window relationships:")
    for older, newer in zip(windows, windows[1:]):
        if older.end == newer.start:
            relation = "contiguous"
        elif older.end > newer.start:
            relation = f"overlap={older.end - newer.start}s"
        else:
            relation = f"gap={newer.start - older.end}s"
        print(f"{older.path.name} -> {newer.path.name}: {relation}")

    if args.index:
        names = [
            line.strip()
            for line in args.index.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        index_windows = sorted(
            (parse_window(Path(name)) for name in names),
            key=lambda window: (window.start, window.end, window.path.name),
        )
        newest_first = names == [window.path.name for window in reversed(index_windows)]
        print(f"Index entries: {len(names):,}")
        print(f"Index order is newest first: {newest_first}")
        positions = {name: position for position, name in enumerate(names, start=1)}
        for window in windows:
            print(f"Index position {positions.get(window.path.name, 'not found')}: {window.path.name}")


if __name__ == "__main__":
    main()
