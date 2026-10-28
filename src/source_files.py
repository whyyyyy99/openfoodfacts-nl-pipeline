"""Shared parsing for Open Food Facts delta source filenames."""

from __future__ import annotations

import re


SOURCE_FILE_NAME_RE = re.compile(
    r"^openfoodfacts_products_(?P<start>\d+)_(?P<end>\d+)\.json\.gz$"
)


def parse_source_file_name(file_name: str) -> tuple[int, int]:
    """Return integer window timestamps from one official delta filename."""
    match = SOURCE_FILE_NAME_RE.fullmatch(file_name)
    if not match:
        raise ValueError(f"invalid Open Food Facts delta filename: {file_name!r}")
    return int(match["start"]), int(match["end"])
