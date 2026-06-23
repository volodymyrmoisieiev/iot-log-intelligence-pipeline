#!/usr/bin/env python3
"""Validate a raw IoT CSV file against the controlled Stage 17 data contract."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ContractConfigurationError(Exception):
    """Raised when the contract file or CLI inputs are invalid."""


@dataclass
class ValidationSummary:
    input_path: str
    contract_path: str
    rows_checked: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    error_count: int = 0
    errors: list[str] | None = None
    stopped_early: bool = False

    def to_json_payload(self) -> dict[str, Any]:
        return {
            "input_path": self.input_path,
            "contract_path": self.contract_path,
            "rows_checked": self.rows_checked,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "errors": self.errors or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a raw IoT CSV file against the Stage 17 data contract."
    )
    parser.add_argument(
        "--contract",
        default="contracts/iot_raw_log_contract.yml",
        help="Path to the data contract YAML file.",
    )
    parser.add_argument("--input", required=True, help="Path to the input CSV file.")
    parser.add_argument(
        "--max-errors",
        type=int,
        default=50,
        help="Maximum number of validation errors to record before stopping.",
    )
    parser.add_argument(
        "--summary-json",
        help="Optional path for a JSON validation summary report.",
    )
    return parser.parse_args()


def validate_paths(args: argparse.Namespace) -> tuple[Path, Path, Path | None]:
    contract_path = Path(args.contract)
    input_path = Path(args.input)
    summary_path = Path(args.summary_json) if args.summary_json else None

    if args.max_errors <= 0:
        raise ContractConfigurationError("--max-errors must be a positive integer.")

    if not contract_path.is_file():
        raise ContractConfigurationError(f"Contract file not found: {contract_path}")

    if not input_path.is_file():
        raise ContractConfigurationError(f"Input CSV not found: {input_path}")

    return contract_path, input_path, summary_path


def parse_scalar(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""

    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]

    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        return int(text)
    except ValueError:
        pass

    try:
        return float(text)
    except ValueError:
        return text


def load_contract(contract_path: Path) -> dict[str, Any]:
    columns: dict[str, dict[str, Any]] = {}
    current_column: dict[str, Any] | None = None
    current_column_name: str | None = None
    current_list_key: str | None = None
    in_columns = False
    in_validation = False

    for line_number, raw_line in enumerate(
        contract_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if "\t" in raw_line:
            raise ContractConfigurationError(
                f"Tabs are not supported in contract YAML: line {line_number}"
            )

        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 0 and stripped == "schema:":
            in_columns = False
            in_validation = False
            continue

        if indent == 2 and stripped == "columns:":
            in_columns = True
            in_validation = False
            continue

        if not in_columns:
            continue

        if indent == 4 and stripped.endswith(":"):
            current_column_name = stripped[:-1]
            current_column = {"validation": {}}
            columns[current_column_name] = current_column
            in_validation = False
            current_list_key = None
            continue

        if current_column is None or current_column_name is None:
            raise ContractConfigurationError(
                f"Malformed contract near line {line_number}: {raw_line}"
            )

        if indent == 6 and stripped == "validation:":
            in_validation = True
            current_list_key = None
            continue

        if indent == 6 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            current_column[key.strip()] = parse_scalar(raw_value)
            in_validation = False
            current_list_key = None
            continue

        if in_validation and indent == 8 and stripped.endswith(":"):
            current_list_key = stripped[:-1]
            current_column["validation"][current_list_key] = []
            continue

        if in_validation and indent == 8 and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            current_column["validation"][key.strip()] = parse_scalar(raw_value)
            current_list_key = None
            continue

        if in_validation and indent == 10 and stripped.startswith("- "):
            if current_list_key is None:
                raise ContractConfigurationError(
                    f"Unexpected list item in contract: line {line_number}"
                )
            current_column["validation"][current_list_key].append(
                parse_scalar(stripped[2:])
            )
            continue

        raise ContractConfigurationError(
            f"Unsupported contract structure at line {line_number}: {raw_line}"
        )

    if not columns:
        raise ContractConfigurationError(
            f"Contract does not define any schema columns: {contract_path}"
        )

    return {"columns": columns}


def normalize_fieldnames(fieldnames: list[str] | None) -> list[str]:
    if not fieldnames:
        raise ContractConfigurationError("Input CSV is missing a header row.")

    return [fieldname.lstrip("\ufeff").strip().strip('"') for fieldname in fieldnames]


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def is_integer_column(column_spec: dict[str, Any]) -> bool:
    return str(column_spec.get("logical_type", "")).lower() == "integer" or str(
        column_spec.get("storage_type", "")
    ).lower() == "integer"


def is_number_column(column_spec: dict[str, Any]) -> bool:
    return str(column_spec.get("logical_type", "")).lower() in {
        "number",
        "float",
        "decimal",
    } or str(column_spec.get("storage_type", "")).lower() in {
        "number",
        "float",
        "decimal",
    }


def parse_numeric_value(value: str, column_name: str, integer_only: bool) -> float | int:
    text = value.strip()

    if integer_only:
        try:
            return int(text)
        except ValueError as exc:
            raise ValueError(
                f"column '{column_name}' expects an integer but got '{value}'"
            ) from exc

    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(
            f"column '{column_name}' expects a number but got '{value}'"
        ) from exc


def value_in_allowed_set(value: str, allowed_values: list[Any]) -> bool:
    normalized_value = value.strip().casefold()
    normalized_allowed = {str(item).strip().casefold() for item in allowed_values}
    return normalized_value in normalized_allowed


def validate_row(
    row_number: int,
    row: dict[str, Any],
    contract_columns: dict[str, dict[str, Any]],
) -> list[str]:
    errors: list[str] = []

    for column_name, column_spec in contract_columns.items():
        validation_rules = column_spec.get("validation", {})
        required = bool(column_spec.get("required", False))
        nullable = bool(column_spec.get("nullable", True))
        raw_value = row.get(column_name)

        if is_empty(raw_value):
            if required and not nullable:
                errors.append(
                    f"Row {row_number}: column '{column_name}' is required and cannot be empty."
                )
            continue

        value = str(raw_value).strip()

        min_length = validation_rules.get("min_length")
        if isinstance(min_length, int) and len(value) < min_length:
            errors.append(
                f"Row {row_number}: column '{column_name}' must be at least {min_length} characters long."
            )

        integer_only = is_integer_column(column_spec)
        numeric_column = integer_only or is_number_column(column_spec)
        numeric_value: float | int | None = None
        if numeric_column:
            try:
                numeric_value = parse_numeric_value(value, column_name, integer_only)
            except ValueError as exc:
                errors.append(f"Row {row_number}: {exc}")
                continue

        min_value = validation_rules.get("min")
        if numeric_value is not None and isinstance(min_value, (int, float)):
            if numeric_value < min_value:
                errors.append(
                    f"Row {row_number}: column '{column_name}' must be >= {min_value}, got {value}."
                )

        allowed_values = validation_rules.get("allowed_values")
        if isinstance(allowed_values, list) and allowed_values:
            if not value_in_allowed_set(value, allowed_values):
                allowed_text = ", ".join(str(item) for item in allowed_values)
                errors.append(
                    f"Row {row_number}: column '{column_name}' has value '{value}' outside allowed values [{allowed_text}]."
                )

    return errors


def append_errors(
    summary: ValidationSummary, row_errors: list[str], max_errors: int
) -> None:
    for error in row_errors:
        if summary.error_count >= max_errors:
            summary.stopped_early = True
            return
        summary.error_count += 1
        assert summary.errors is not None
        summary.errors.append(error)


def validate_csv(
    contract: dict[str, Any], input_path: Path, contract_path: Path, max_errors: int
) -> ValidationSummary:
    summary = ValidationSummary(
        input_path=str(input_path),
        contract_path=str(contract_path),
        errors=[],
    )
    contract_columns = contract["columns"]
    required_columns = [
        column_name
        for column_name, column_spec in contract_columns.items()
        if column_spec.get("required", False)
    ]

    with input_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = normalize_fieldnames(reader.fieldnames)
        reader.fieldnames = fieldnames

        missing_columns = [
            column_name for column_name in required_columns if column_name not in fieldnames
        ]
        if missing_columns:
            append_errors(
                summary,
                [
                    "Input CSV is missing required columns: "
                    + ", ".join(missing_columns)
                ],
                max_errors,
            )
            return summary

        for row_number, row in enumerate(reader, start=2):
            summary.rows_checked += 1
            row_errors = validate_row(
                row_number=row_number,
                row=row,
                contract_columns=contract_columns,
            )

            if row_errors:
                summary.invalid_rows += 1
                append_errors(summary, row_errors, max_errors)
                if summary.stopped_early:
                    break
            else:
                summary.valid_rows += 1

    return summary


def print_summary(summary: ValidationSummary) -> None:
    print("Data contract validation summary")
    print(f"Input file: {summary.input_path}")
    print(f"Contract file: {summary.contract_path}")
    print(f"Rows checked: {summary.rows_checked}")
    print(f"Valid rows: {summary.valid_rows}")
    print(f"Invalid rows: {summary.invalid_rows}")
    print(f"Error count: {summary.error_count}")
    if summary.stopped_early:
        print("Stopped early: reached --max-errors limit.")


def print_errors(summary: ValidationSummary) -> None:
    if not summary.errors:
        return

    print("Validation errors:", file=sys.stderr)
    for error in summary.errors:
        print(f"- {error}", file=sys.stderr)
    if summary.stopped_early:
        print(
            "- Validation stopped after reaching the configured --max-errors limit.",
            file=sys.stderr,
        )


def write_summary_json(summary_path: Path, summary: ValidationSummary) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary.to_json_payload(), indent=2),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()

    try:
        contract_path, input_path, summary_path = validate_paths(args)
        contract = load_contract(contract_path)
        summary = validate_csv(
            contract=contract,
            input_path=input_path,
            contract_path=contract_path,
            max_errors=args.max_errors,
        )

        print_summary(summary)
        if summary.errors:
            print_errors(summary)

        if summary_path is not None:
            write_summary_json(summary_path, summary)

        return 0 if summary.error_count == 0 else 1
    except ContractConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"Input/output error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
