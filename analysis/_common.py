"""Shared, standard-library helpers for profiling Open Food Facts delta files."""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence


FILENAME_RE = re.compile(
    r"^openfoodfacts_products_(?P<start>\d+)_(?P<end>\d+)\.json\.gz$"
)


@dataclass(frozen=True)
class Window:
    path: Path
    start: int
    end: int


@dataclass(frozen=True)
class ParsedLine:
    window: Window
    line_number: int
    raw_record: bytes
    value: Any | None
    error: str | None


def parse_window(path: Path) -> Window:
    match = FILENAME_RE.fullmatch(path.name)
    if not match:
        raise ValueError(f"Unexpected delta filename: {path.name}")
    return Window(path=path, start=int(match["start"]), end=int(match["end"]))


def ordered_windows(paths: Sequence[Path]) -> list[Window]:
    windows = [parse_window(path) for path in paths]
    if len({window.path.resolve() for window in windows}) != len(windows):
        raise ValueError("The same input path was supplied more than once")
    missing = [str(window.path) for window in windows if not window.path.is_file()]
    if missing:
        raise FileNotFoundError("Missing input files: " + ", ".join(missing))
    return sorted(windows, key=lambda window: (window.start, window.end, window.path.name))


def iter_lines(windows: Sequence[Window]) -> Iterator[ParsedLine]:
    for window in windows:
        with gzip.open(window.path, "rb") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                raw_record = raw_line.rstrip(b"\r\n")
                try:
                    value = json.loads(raw_record)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    yield ParsedLine(
                        window=window,
                        line_number=line_number,
                        raw_record=raw_record,
                        value=None,
                        error=type(exc).__name__,
                    )
                    continue
                yield ParsedLine(
                    window=window,
                    line_number=line_number,
                    raw_record=raw_record,
                    value=value,
                    error=None,
                )


def type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__


def numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def ratio(numerator: int, denominator: int) -> str:
    percentage = numerator / denominator * 100 if denominator else 0
    return f"{numerator:,} of {denominator:,} ({percentage:.2f}%)"
