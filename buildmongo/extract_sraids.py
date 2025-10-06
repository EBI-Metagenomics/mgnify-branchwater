#!/usr/bin/env python3
"""
extract_sraids.py

A small utility script that:
- Accepts a path to a manifest.csv file.
- Extracts the values from the first column, starting from the 3rd row (i.e., skip first two rows).
- Writes those values (one per line) into a file named "sraids" at the provided destination path.
- If the destination file already exists, it will be overwritten.

Usage:
  python buildmongo/extract_sraids.py /path/to/manifest.csv /path/to/destination

Notes:
- If the destination is a directory, the output will be written to DESTINATION/sraids
- If the destination is a file path and its basename is "sraids", the script will write to that file directly.
- The script will create the destination directory when a directory path is provided and does not exist.
"""

import argparse
import csv
import os
import sys
from pathlib import Path


def determine_output_path(dest_arg: str) -> Path:
    """Determine the output path for the sraids file.

    - If dest_arg is an existing directory, return dest/sraids
    - If dest_arg ends with a path component named 'sraids' (file), return that path
    - Otherwise, treat dest_arg as a directory path and return dest_arg/sraids
    """
    dest_path = Path(dest_arg)
    if dest_path.exists() and dest_path.is_dir():
        return dest_path / "sraids"

    # If a file path explicitly named 'sraids' is provided, respect it
    if dest_path.name == "sraids":
        parent = dest_path.parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        return dest_path

    # Otherwise, it's intended as a directory path
    if not dest_path.exists():
        dest_path.mkdir(parents=True, exist_ok=True)
    elif not dest_path.is_dir():
        # Exists but is not a directory and not named 'sraids' -> error
        raise ValueError(
            f"Destination '{dest_arg}' exists and is not a directory. "
            "Provide a directory or a file path named 'sraids'."
        )
    return dest_path / "sraids"


def extract_first_column_from_third_row(manifest_path: Path) -> list[str]:
    """Read a CSV file, skip first two rows, and return values from the first column.

    - Ignores empty rows.
    - Strips whitespace from the extracted values.
    """
    rows = []
    # Use utf-8-sig to gracefully handle BOM if present
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        # Skip first two rows
        for _ in range(2):
            try:
                next(reader)
            except StopIteration:
                # If the file has fewer than 3 rows, nothing to return
                return []
        for rec in reader:
            if not rec:
                continue
            first = (rec[0] if len(rec) > 0 else "").strip()
            # Remove trailing .sig extension if present (case-insensitive)
            if first.lower().endswith('.sig'):
                first = first[:-4]
            if first:
                rows.append(first)
    return rows


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Extract first-column values from the 3rd row onward of a CSV and write to a 'sraids' file."
        )
    )
    parser.add_argument(
        "manifest",
        type=str,
        help="Path to manifest.csv"
    )
    parser.add_argument(
        "destination",
        type=str,
        help=(
            "Destination directory or file path. If directory, output is DESTINATION/sraids. "
            "If file path and its basename is 'sraids', output is written there."
        ),
    )

    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        print(f"Error: manifest file not found: {manifest_path}", file=sys.stderr)
        return 2

    try:
        out_path = determine_output_path(args.destination)
    except Exception as e:
        print(f"Error determining destination: {e}", file=sys.stderr)
        return 2

    try:
        values = extract_first_column_from_third_row(manifest_path)
    except Exception as e:
        print(f"Error reading manifest: {e}", file=sys.stderr)
        return 1

    try:
        # Overwrite if exists
        with out_path.open("w", encoding="utf-8", newline="\n") as w:
            for i, val in enumerate(values):
                # Ensure one value per line; avoid extra trailing blank lines by consistent newline handling
                w.write(val)
                if i < len(values) - 1:
                    w.write("\n")
        # Ensure file ends with newline (optional, but often desirable)
        # If there were values, the last line already has no newline; we can add one for POSIX friendliness
        if values:
            with out_path.open("a", encoding="utf-8", newline="\n") as w:
                w.write("\n")
    except Exception as e:
        print(f"Error writing output: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
