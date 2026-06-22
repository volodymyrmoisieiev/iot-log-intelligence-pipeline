#!/usr/bin/env python3
"""Create a smaller dataset profile CSV from a larger IoT CSV source."""

from __future__ import annotations

import argparse
import csv
import random
import sys
from pathlib import Path
from typing import Iterable, List, Sequence


REQUIRED_COLUMNS = [
    "event_timestamp",
    "device_id",
    "source_ip",
    "destination_ip",
    "protocol",
    "packet_size",
    "duration_ms",
    "event_type",
    "attack_type",
    "status",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a dataset profile CSV from a larger IoT CSV input."
    )
    parser.add_argument("--input", required=True, help="Path to the source CSV file.")
    parser.add_argument("--output", required=True, help="Path to the output CSV file.")
    parser.add_argument(
        "--rows",
        type=int,
        default=10000,
        help="Maximum number of data rows to write. Default: 10000.",
    )
    parser.add_argument(
        "--mode",
        choices=("first", "random"),
        default="first",
        help="Row selection mode. Default: first.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used when mode=random. Default: 42.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing output file.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> tuple[Path, Path]:
    input_path = Path(args.input)
    output_path = Path(args.output)

    if args.rows <= 0:
        raise ValueError("--rows must be a positive integer.")

    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    if output_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Output file already exists: {output_path}. "
            "Use --overwrite to replace it."
        )

    if input_path.resolve() == output_path.resolve():
        raise ValueError("Input and output paths must be different.")

    return input_path, output_path


def ensure_required_columns(fieldnames: Sequence[str] | None) -> List[str]:
    if not fieldnames:
        raise ValueError("Input CSV is missing a header row.")

    normalized_fieldnames = [
        fieldname.lstrip("\ufeff").strip().strip('"') for fieldname in fieldnames
    ]
    missing = [
        column for column in REQUIRED_COLUMNS if column not in normalized_fieldnames
    ]
    if missing:
        raise ValueError(
            "Input CSV is missing required columns: " + ", ".join(missing)
        )

    return normalized_fieldnames


def select_first_rows(reader: csv.DictReader, max_rows: int) -> List[dict[str, str]]:
    rows: List[dict[str, str]] = []
    for index, row in enumerate(reader):
        if index >= max_rows:
            break
        rows.append(row)
    return rows


def select_random_rows(
    reader: csv.DictReader, max_rows: int, seed: int
) -> List[dict[str, str]]:
    rng = random.Random(seed)
    reservoir: List[dict[str, str]] = []

    for index, row in enumerate(reader):
        if index < max_rows:
            reservoir.append(row)
            continue

        replacement_index = rng.randint(0, index)
        if replacement_index < max_rows:
            reservoir[replacement_index] = row

    return reservoir


def write_rows(
    output_path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, str]]
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            row_count += 1

    return row_count


def print_summary(
    input_path: Path, output_path: Path, requested_rows: int, written_rows: int, mode: str
) -> None:
    print("Dataset profile creation summary")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Requested rows: {requested_rows}")
    print(f"Written rows: {written_rows}")
    print(f"Mode: {mode}")


def main() -> int:
    args = parse_args()

    try:
        input_path, output_path = validate_args(args)
        with input_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = ensure_required_columns(reader.fieldnames)
            reader.fieldnames = fieldnames

            if args.mode == "first":
                selected_rows = select_first_rows(reader, args.rows)
            else:
                selected_rows = select_random_rows(reader, args.rows, args.seed)

        written_rows = write_rows(output_path, fieldnames, selected_rows)
        print_summary(
            input_path=input_path,
            output_path=output_path,
            requested_rows=args.rows,
            written_rows=written_rows,
            mode=args.mode,
        )
        return 0
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
