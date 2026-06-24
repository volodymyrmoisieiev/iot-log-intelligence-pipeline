#!/usr/bin/env python3
"""Run a safe local end-to-end smoke test foundation for the repository."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PROFILES_PATH = PROJECT_ROOT / "data" / "dataset_profiles.yml"
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "iot_raw_log_contract.yml"
AIRFLOW_DAG_DIR = PROJECT_ROOT / "airflow" / "dags"
TERRAFORM_ROOT = PROJECT_ROOT / "infra" / "aws-orchestration"
DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = "5432"
DEFAULT_POSTGRES_DB = "iot_logs"
DEFAULT_POSTGRES_USER = "iot_user"
DEFAULT_POSTGRES_PASSWORD = "iot_password"
KAFKA_TOPIC_TOOL_PATH = "/opt/kafka/bin/kafka-topics.sh"

REQUIRED_DIRECTORIES = [
    "airflow",
    "aws",
    "contracts",
    "data",
    "dbt",
    "docs",
    "infra/aws-orchestration",
    "scripts",
]

REQUIRED_FILES = [
    "README.md",
    "docker-compose.yml",
    "data/dataset_profiles.yml",
    "contracts/iot_raw_log_contract.yml",
    "scripts/validate_data_contract.py",
    "scripts/run_anomaly_detection.py",
]

PYTHON_ENTRY_POINTS = [
    "scripts/run_local_e2e_smoke_test.py",
    "scripts/run_anomaly_detection.py",
    "scripts/validate_data_contract.py",
    "scripts/run_performance_benchmark.py",
    "scripts/analyze_performance_results.py",
    "scripts/create_dataset_profile.py",
    "aws/lambda/iot_metadata_validator/handler.py",
    "observability/write_pipeline_observability.py",
]

AIRFLOW_PYTHON_FILES = [
    "airflow/dags/iot_local_pipeline_dag.py",
    "airflow/dags/iot_pipeline_smoke_dag.py",
]

DBT_FOUNDATION_FILES = [
    "dbt/dbt_project.yml",
    "dbt/profiles.yml",
    "dbt/models/sources.yml",
]

MISSING_RUNTIME_HINTS = (
    "docker cli was not found",
    "is not running",
    "start postgresql first",
    "cannot connect to the docker daemon",
    "failed to complete postgresql read query",
    "service \"postgres\" is not running",
    "no such container",
)

DOCKER_PERMISSION_HINTS = (
    "permission denied while trying to connect to the docker api",
    "cannot connect to the docker daemon",
    "access is denied",
)


class SmokeTestConfigurationError(Exception):
    """Raised when smoke test inputs are invalid."""


@dataclass
class CheckResult:
    name: str
    status: str
    required: bool
    details: str
    duration_seconds: float = 0.0
    command: list[str] | None = None
    return_code: int | None = None
    stdout_excerpt: str | None = None
    stderr_excerpt: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a safe local E2E smoke test foundation without starting the full "
            "Kafka/PostgreSQL pipeline or deploying AWS resources."
        )
    )
    parser.add_argument(
        "--profile",
        choices=["sample", "medium", "full"],
        default="sample",
        help="Dataset profile to reference. Default: sample.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=1000,
        help="Maximum number of dataset rows to inspect for bounded checks. Default: 1000.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which external checks would run without executing them.",
    )
    parser.add_argument(
        "--output-json",
        help="Optional path for a JSON smoke test summary.",
    )
    parser.add_argument(
        "--run-sample-pipeline",
        action="store_true",
        help=(
            "Run a controlled sample-profile producer/consumer/warehouse-loader flow "
            "with bounded row limits."
        ),
    )
    parser.add_argument(
        "--skip-airflow",
        action="store_true",
        help="Skip Airflow DAG-specific syntax checks.",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt foundation file checks.",
    )
    parser.add_argument(
        "--skip-anomaly-detection",
        action="store_true",
        help="Skip the optional read-only anomaly detection helper check.",
    )
    parser.add_argument(
        "--skip-terraform",
        action="store_true",
        help="Skip Terraform init/validate checks under infra/aws-orchestration.",
    )
    return parser.parse_args()


def parse_scalar(value: str) -> str | bool:
    normalized = value.strip()
    if not normalized:
        return ""
    lowered = normalized.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return normalized.strip("'\"")


def load_dataset_profiles(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        raise SmokeTestConfigurationError(f"Dataset profiles file not found: {path}")

    profiles: dict[str, dict[str, Any]] = {}
    current_profile: str | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent == 2 and stripped.endswith(":"):
            current_profile = stripped[:-1]
            profiles[current_profile] = {}
            continue

        if indent == 4 and current_profile and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            profiles[current_profile][key.strip()] = parse_scalar(raw_value)

    if not profiles:
        raise SmokeTestConfigurationError(
            f"No dataset profiles could be parsed from {path}."
        )
    return profiles


def ensure_args_are_valid(args: argparse.Namespace) -> None:
    if args.max_rows <= 0:
        raise SmokeTestConfigurationError("--max-rows must be a positive integer.")
    if args.run_sample_pipeline and args.profile != "sample":
        raise SmokeTestConfigurationError(
            "Stage 21B controlled runtime mode currently supports only --profile sample. "
            "Larger-profile validation is reserved for later Stage 21C/21D work."
        )


def make_result(
    *,
    name: str,
    status: str,
    required: bool,
    details: str,
    start_time: float | None = None,
    command: list[str] | None = None,
    return_code: int | None = None,
    stdout_excerpt: str | None = None,
    stderr_excerpt: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> CheckResult:
    duration = 0.0 if start_time is None else round(time.time() - start_time, 3)
    return CheckResult(
        name=name,
        status=status,
        required=required,
        details=details,
        duration_seconds=duration,
        command=command,
        return_code=return_code,
        stdout_excerpt=stdout_excerpt,
        stderr_excerpt=stderr_excerpt,
        metadata=metadata or {},
    )


def shorten_output(text: str | None, *, max_lines: int = 20, max_chars: int = 4000) -> str | None:
    if text is None:
        return None
    normalized = text.strip()
    if not normalized:
        return None
    lines = normalized.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
        normalized = "\n".join(lines)
        normalized = "[...]\n" + normalized
    else:
        normalized = "\n".join(lines)

    if len(normalized) > max_chars:
        normalized = "[...]" + normalized[-(max_chars - 5) :]
    return normalized


def is_docker_permission_issue(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(hint in lowered for hint in DOCKER_PERMISSION_HINTS)


def count_dataset_rows(dataset_path: Path, max_rows: int) -> int:
    with dataset_path.open("r", newline="", encoding="utf-8-sig") as source_file:
        reader = csv.reader(source_file)
        header = next(reader, None)
        if header is None:
            raise SmokeTestConfigurationError(
                f"Dataset file is missing a header row: {dataset_path}"
            )

        row_count = 0
        for _ in reader:
            row_count += 1
            if row_count >= max_rows:
                break
        return row_count


def build_runtime_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}-{os.getpid()}"


def create_pipeline_runtime_context(max_rows: int) -> dict[str, Any]:
    runtime_id = build_runtime_id("stage21b-sample")
    topic_suffix = runtime_id.replace("-", "_")
    raw_topic = f"iot_raw_logs_{topic_suffix}"
    processed_topic = f"iot_processed_logs_{topic_suffix}"
    invalid_topic = f"iot_invalid_logs_{topic_suffix}"
    return {
        "runtime_id": runtime_id,
        "raw_topic": raw_topic,
        "processed_topic": processed_topic,
        "invalid_topic": invalid_topic,
        "consumer_group_id": f"{runtime_id}-consumer",
        "loader_group_id": f"{runtime_id}-loader",
        "max_rows": max_rows,
    }


def run_command_sequence(
    *,
    name: str,
    commands: list[list[str]],
    required: bool,
    cwd: Path = PROJECT_ROOT,
    env: dict[str, str] | None = None,
    dry_run: bool,
    success_detail: str,
    dry_run_detail: str,
    metadata: dict[str, Any] | None = None,
) -> CheckResult:
    start_time = time.time()
    if dry_run:
        return make_result(
            name=name,
            status="dry_run",
            required=required,
            details=dry_run_detail,
            start_time=start_time,
            metadata={"commands": commands, **(metadata or {})},
        )

    combined_stdout: list[str] = []
    combined_stderr: list[str] = []

    for command in commands:
        try:
            result = subprocess.run(
                command,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            return make_result(
                name=name,
                status="failed",
                required=required,
                details=f"Command not found: {command[0]} ({exc})",
                start_time=start_time,
                command=command,
                metadata=metadata,
            )

        if result.stdout:
            combined_stdout.append("$ " + " ".join(command))
            combined_stdout.append(result.stdout.strip())
        if result.stderr:
            combined_stderr.append("$ " + " ".join(command))
            combined_stderr.append(result.stderr.strip())

        if result.returncode != 0:
            return make_result(
                name=name,
                status="failed",
                required=required,
                details="Command sequence returned a non-zero exit code.",
                start_time=start_time,
                command=command,
                return_code=result.returncode,
                stdout_excerpt=shorten_output("\n".join(combined_stdout)),
                stderr_excerpt=shorten_output("\n".join(combined_stderr)),
                metadata=metadata,
            )

    return make_result(
        name=name,
        status="passed",
        required=required,
        details=success_detail,
        start_time=start_time,
        stdout_excerpt=shorten_output("\n".join(combined_stdout)),
        stderr_excerpt=shorten_output("\n".join(combined_stderr)),
        metadata=metadata,
    )


def build_postgres_count_command() -> list[str]:
    postgres_user = os.getenv("POSTGRES_USER", DEFAULT_POSTGRES_USER)
    postgres_db = os.getenv("POSTGRES_DB", DEFAULT_POSTGRES_DB)
    postgres_password = os.getenv("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD)
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        f"PGPASSWORD={postgres_password}",
        "postgres",
        "psql",
        "-U",
        postgres_user,
        "-d",
        postgres_db,
        "-t",
        "-A",
        "-F",
        ",",
        "-c",
        (
            "SELECT "
            "(SELECT COUNT(*) FROM processed_iot_logs) AS processed_count, "
            "(SELECT COUNT(*) FROM invalid_iot_logs) AS invalid_count;"
        ),
    ]


def read_postgres_counts() -> tuple[CheckResult, dict[str, int] | None]:
    start_time = time.time()
    command = build_postgres_count_command()

    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        return (
            make_result(
                name="sample_pipeline_postgres_counts",
                status="failed",
                required=True,
                details=f"Command not found: {command[0]} ({exc})",
                start_time=start_time,
                command=command,
            ),
            None,
        )

    if result.returncode != 0:
        return (
            make_result(
                name="sample_pipeline_postgres_counts",
                status="failed",
                required=True,
                details="Failed to read PostgreSQL row counts after the controlled runtime flow.",
                start_time=start_time,
                command=command,
                return_code=result.returncode,
                stdout_excerpt=shorten_output(result.stdout),
                stderr_excerpt=shorten_output(result.stderr),
            ),
            None,
        )

    raw_output = (result.stdout or "").strip().splitlines()
    if not raw_output:
        return (
            make_result(
                name="sample_pipeline_postgres_counts",
                status="failed",
                required=True,
                details="PostgreSQL row-count query returned no output.",
                start_time=start_time,
                command=command,
                stdout_excerpt=shorten_output(result.stdout),
                stderr_excerpt=shorten_output(result.stderr),
            ),
            None,
        )

    last_line = raw_output[-1].strip()
    parts = last_line.split(",")
    if len(parts) != 2:
        return (
            make_result(
                name="sample_pipeline_postgres_counts",
                status="failed",
                required=True,
                details="PostgreSQL row-count query returned an unexpected format.",
                start_time=start_time,
                command=command,
                stdout_excerpt=shorten_output(result.stdout),
                stderr_excerpt=shorten_output(result.stderr),
            ),
            None,
        )

    try:
        counts = {
            "processed_count": int(parts[0].strip()),
            "invalid_count": int(parts[1].strip()),
        }
    except ValueError:
        return (
            make_result(
                name="sample_pipeline_postgres_counts",
                status="failed",
                required=True,
                details="PostgreSQL row-count query returned non-integer values.",
                start_time=start_time,
                command=command,
                stdout_excerpt=shorten_output(result.stdout),
                stderr_excerpt=shorten_output(result.stderr),
            ),
            None,
        )

    return (
        make_result(
            name="sample_pipeline_postgres_counts",
            status="passed",
            required=True,
            details="Captured PostgreSQL processed/invalid row counts.",
            start_time=start_time,
            command=command,
            return_code=result.returncode,
            stdout_excerpt=shorten_output(result.stdout),
            stderr_excerpt=shorten_output(result.stderr),
            metadata=counts,
        ),
        counts,
    )


def run_controlled_sample_pipeline(
    *,
    dataset_path: Path,
    max_rows: int,
    dry_run: bool,
    skip_anomaly_detection: bool,
) -> list[CheckResult]:
    expected_rows = count_dataset_rows(dataset_path=dataset_path, max_rows=max_rows)
    context = create_pipeline_runtime_context(max_rows=max_rows)
    context["expected_dataset_rows"] = expected_rows

    if expected_rows == 0:
        return [
            make_result(
                name="sample_pipeline_runtime",
                status="failed",
                required=True,
                details="The selected sample dataset has no data rows to exercise in runtime mode.",
                metadata=context,
            )
        ]

    results: list[CheckResult] = []
    results.append(
        run_command(
            name="sample_pipeline_start_services",
            command=["docker", "compose", "up", "-d", "kafka", "kafka-init", "postgres"],
            required=True,
            dry_run=dry_run,
            success_detail="Started required Docker Compose services for the controlled sample runtime flow.",
            dry_run_detail=(
                "Would start kafka, kafka-init, and postgres for the controlled sample runtime flow."
            ),
            env=None,
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    topic_commands = [
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            KAFKA_TOPIC_TOOL_PATH,
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic_name,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]
        for topic_name in (
            context["raw_topic"],
            context["processed_topic"],
            context["invalid_topic"],
        )
    ]
    results.append(
        run_command_sequence(
            name="sample_pipeline_create_topics",
            commands=topic_commands,
            required=True,
            dry_run=dry_run,
            success_detail="Created isolated Kafka topics for the controlled sample runtime flow.",
            dry_run_detail="Would create isolated Kafka topics for raw, processed, and invalid sample-runtime messages.",
            metadata={
                "raw_topic": context["raw_topic"],
                "processed_topic": context["processed_topic"],
                "invalid_topic": context["invalid_topic"],
            },
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    pre_counts_result, pre_counts = read_postgres_counts() if not dry_run else (
        make_result(
            name="sample_pipeline_pre_run_postgres_counts",
            status="dry_run",
            required=True,
            details="Would capture PostgreSQL row counts before running the controlled sample pipeline.",
            metadata=context,
        ),
        None,
    )
    pre_counts_result.name = "sample_pipeline_pre_run_postgres_counts"
    results.append(pre_counts_result)
    if not dry_run and results[-1].status == "failed":
        return results

    producer_command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--build",
        "-e",
        "DATASET_PROFILE=sample",
        "-e",
        f"KAFKA_RAW_TOPIC={context['raw_topic']}",
        "-e",
        f"PRODUCER_MAX_ROWS={expected_rows}",
        "-e",
        "PRODUCER_SEND_DELAY_MS=0",
        "go-producer",
    ]
    results.append(
        run_command(
            name="sample_pipeline_producer",
            command=producer_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the Go producer against the sample profile with a row limit of {expected_rows}."
            ),
            dry_run_detail=(
                f"Would run the Go producer with DATASET_PROFILE=sample, PRODUCER_MAX_ROWS={expected_rows}, "
                "and PRODUCER_SEND_DELAY_MS=0."
            ),
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    consumer_command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--build",
        "-e",
        f"KAFKA_RAW_TOPIC={context['raw_topic']}",
        "-e",
        f"KAFKA_PROCESSED_TOPIC={context['processed_topic']}",
        "-e",
        f"KAFKA_INVALID_TOPIC={context['invalid_topic']}",
        "-e",
        f"CONSUMER_GROUP_ID={context['consumer_group_id']}",
        "-e",
        f"CONSUMER_MAX_MESSAGES={expected_rows}",
        "-e",
        "CONSUMER_PROGRESS_INTERVAL=250",
        "python-consumer",
    ]
    results.append(
        run_command(
            name="sample_pipeline_consumer",
            command=consumer_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the Python consumer with CONSUMER_MAX_MESSAGES={expected_rows} on the isolated sample topics."
            ),
            dry_run_detail=(
                f"Would run the Python consumer with CONSUMER_MAX_MESSAGES={expected_rows} and isolated Kafka topics."
            ),
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    loader_command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--build",
        "-e",
        f"KAFKA_PROCESSED_TOPIC={context['processed_topic']}",
        "-e",
        f"KAFKA_INVALID_TOPIC={context['invalid_topic']}",
        "-e",
        f"WAREHOUSE_LOADER_GROUP_ID={context['loader_group_id']}",
        "-e",
        f"WAREHOUSE_LOADER_MAX_MESSAGES={expected_rows}",
        "-e",
        "WAREHOUSE_LOADER_PROGRESS_INTERVAL=250",
        "warehouse-loader",
    ]
    results.append(
        run_command(
            name="sample_pipeline_warehouse_loader",
            command=loader_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the warehouse loader with WAREHOUSE_LOADER_MAX_MESSAGES={expected_rows} on the isolated sample topics."
            ),
            dry_run_detail=(
                f"Would run the warehouse loader with WAREHOUSE_LOADER_MAX_MESSAGES={expected_rows} and isolated Kafka topics."
            ),
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    if dry_run:
        results.append(
            make_result(
                name="sample_pipeline_verify_postgres_deltas",
                status="dry_run",
                required=True,
                details="Would compare PostgreSQL row-count deltas after the controlled sample runtime flow.",
                metadata=context,
            )
        )
    else:
        post_counts_result, post_counts = read_postgres_counts()
        post_counts_result.name = "sample_pipeline_post_run_postgres_counts"
        results.append(post_counts_result)
        if results[-1].status == "failed":
            return results

        if pre_counts is not None and post_counts is not None:
            processed_delta = post_counts["processed_count"] - pre_counts["processed_count"]
            invalid_delta = post_counts["invalid_count"] - pre_counts["invalid_count"]
            total_delta = processed_delta + invalid_delta
            details = (
                f"PostgreSQL row-count delta after the controlled sample runtime flow: "
                f"processed_delta={processed_delta}, invalid_delta={invalid_delta}, total_delta={total_delta}, "
                f"expected_rows={expected_rows}."
            )
            status = "passed" if total_delta == expected_rows and processed_delta >= 0 and invalid_delta >= 0 else "failed"
            if total_delta != expected_rows:
                details = (
                    f"Controlled sample runtime loaded {total_delta} total rows into PostgreSQL, "
                    f"but expected {expected_rows} rows from the bounded sample dataset."
                )
            results.append(
                make_result(
                    name="sample_pipeline_verify_postgres_deltas",
                    status=status,
                    required=True,
                    details=details,
                    metadata={
                        **context,
                        "pre_counts": pre_counts,
                        "post_counts": post_counts,
                        "processed_delta": processed_delta,
                        "invalid_delta": invalid_delta,
                        "total_delta": total_delta,
                    },
                )
            )

    anomaly_result = run_anomaly_detection_check(
        skip_anomaly_detection=skip_anomaly_detection,
        max_rows=expected_rows,
        dry_run=dry_run,
        check_name="sample_pipeline_anomaly_detection_read_only",
    )
    results.append(anomaly_result)
    return results


def run_command(
    *,
    name: str,
    command: list[str],
    required: bool,
    cwd: Path = PROJECT_ROOT,
    env: dict[str, str] | None = None,
    dry_run: bool,
    success_detail: str,
    dry_run_detail: str,
) -> CheckResult:
    start_time = time.time()
    if dry_run:
        return make_result(
            name=name,
            status="dry_run",
            required=required,
            details=dry_run_detail,
            start_time=start_time,
            command=command,
        )

    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        return make_result(
            name=name,
            status="failed",
            required=required,
            details=f"Command not found: {command[0]} ({exc})",
            start_time=start_time,
            command=command,
        )

    status = "passed" if result.returncode == 0 else "failed"
    details = success_detail if result.returncode == 0 else "Command returned a non-zero exit code."
    return make_result(
        name=name,
        status=status,
        required=required,
        details=details,
        start_time=start_time,
        command=command,
        return_code=result.returncode,
        stdout_excerpt=shorten_output(result.stdout),
        stderr_excerpt=shorten_output(result.stderr),
    )


def check_expected_repository_structure() -> CheckResult:
    start_time = time.time()
    missing_directories = [
        directory
        for directory in REQUIRED_DIRECTORIES
        if not (PROJECT_ROOT / directory).is_dir()
    ]
    missing_files = [
        file_path for file_path in REQUIRED_FILES if not (PROJECT_ROOT / file_path).is_file()
    ]

    if missing_directories or missing_files:
        missing_bits: list[str] = []
        if missing_directories:
            missing_bits.append("missing directories: " + ", ".join(missing_directories))
        if missing_files:
            missing_bits.append("missing files: " + ", ".join(missing_files))
        return make_result(
            name="expected_repository_structure",
            status="failed",
            required=True,
            details="; ".join(missing_bits),
            start_time=start_time,
        )

    return make_result(
        name="expected_repository_structure",
        status="passed",
        required=True,
        details="Expected top-level directories and core repository files are present.",
        start_time=start_time,
    )


def check_selected_dataset(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
) -> CheckResult:
    start_time = time.time()
    if not dataset_path.is_file():
        return make_result(
            name="selected_dataset_exists",
            status="failed",
            required=True,
            details=f"Dataset file for profile '{profile_name}' was not found: {dataset_path}",
            start_time=start_time,
            metadata={"profile": profile_name, "dataset_path": str(dataset_path)},
        )

    return make_result(
        name="selected_dataset_exists",
        status="passed",
        required=True,
        details=(
            f"Dataset file for profile '{profile_name}' exists and the smoke test will cap "
            f"row-oriented checks at {max_rows} rows."
        ),
        start_time=start_time,
        metadata={"profile": profile_name, "dataset_path": str(dataset_path)},
    )


def check_dbt_foundation(skip_dbt: bool) -> CheckResult:
    start_time = time.time()
    if skip_dbt:
        return make_result(
            name="dbt_foundation_files",
            status="skipped",
            required=False,
            details="Skipped because --skip-dbt was provided.",
            start_time=start_time,
        )

    missing_files = [
        file_path for file_path in DBT_FOUNDATION_FILES if not (PROJECT_ROOT / file_path).is_file()
    ]
    if missing_files:
        return make_result(
            name="dbt_foundation_files",
            status="failed",
            required=False,
            details="Missing dbt foundation files: " + ", ".join(missing_files),
            start_time=start_time,
        )

    return make_result(
        name="dbt_foundation_files",
        status="passed",
        required=False,
        details="dbt project foundation files are present.",
        start_time=start_time,
    )


def check_python_syntax(skip_airflow: bool) -> CheckResult:
    import py_compile

    start_time = time.time()
    files_to_compile = list(PYTHON_ENTRY_POINTS)
    if skip_airflow:
        skipped_airflow_files = list(AIRFLOW_PYTHON_FILES)
    else:
        files_to_compile.extend(AIRFLOW_PYTHON_FILES)
        skipped_airflow_files = []

    missing_files = [
        file_path for file_path in files_to_compile if not (PROJECT_ROOT / file_path).is_file()
    ]
    if missing_files:
        return make_result(
            name="python_syntax",
            status="failed",
            required=True,
            details="Missing Python files expected for compilation: " + ", ".join(missing_files),
            start_time=start_time,
        )

    compiled_files: list[str] = []
    try:
        for file_path in files_to_compile:
            py_compile.compile(str(PROJECT_ROOT / file_path), doraise=True)
            compiled_files.append(file_path)
    except py_compile.PyCompileError as exc:
        return make_result(
            name="python_syntax",
            status="failed",
            required=True,
            details=f"Python syntax compilation failed: {exc.msg}",
            start_time=start_time,
            metadata={"compiled_files": compiled_files},
        )

    details = f"Compiled {len(compiled_files)} Python files successfully."
    metadata: dict[str, Any] = {"compiled_files": compiled_files}
    if skipped_airflow_files:
        details += " Airflow DAG syntax was skipped."
        metadata["skipped_airflow_files"] = skipped_airflow_files

    return make_result(
        name="python_syntax",
        status="passed",
        required=True,
        details=details,
        start_time=start_time,
        metadata=metadata,
    )


def prepare_bounded_dataset_copy(dataset_path: Path, max_rows: int) -> tuple[Path, int]:
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        suffix=".csv",
        delete=False,
    )
    temp_path = Path(temp_file.name)
    copied_rows = 0

    try:
        with dataset_path.open("r", newline="", encoding="utf-8-sig") as source_file:
            reader = csv.reader(source_file)
            writer = csv.writer(temp_file)

            header = next(reader, None)
            if header is None:
                raise SmokeTestConfigurationError(
                    f"Dataset file is missing a header row: {dataset_path}"
                )
            writer.writerow(header)

            for row in reader:
                writer.writerow(row)
                copied_rows += 1
                if copied_rows >= max_rows:
                    break
    finally:
        temp_file.close()

    return temp_path, copied_rows


def run_data_contract_validation(
    *,
    dataset_path: Path,
    max_rows: int,
    dry_run: bool,
) -> CheckResult:
    start_time = time.time()
    bounded_dataset_path: Path | None = None
    temp_summary_path: Path | None = None

    try:
        bounded_dataset_path, copied_rows = prepare_bounded_dataset_copy(
            dataset_path=dataset_path,
            max_rows=max_rows,
        )
        temp_summary_file = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        temp_summary_path = Path(temp_summary_file.name)
        temp_summary_file.close()
        command = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "validate_data_contract.py"),
            "--contract",
            str(CONTRACT_PATH),
            "--input",
            str(bounded_dataset_path),
            "--summary-json",
            str(temp_summary_path),
        ]

        if dry_run:
            return make_result(
                name="data_contract_validation",
                status="dry_run",
                required=False,
                details=(
                    f"Would validate up to {max_rows} rows from the selected dataset against "
                    "the Stage 17 contract."
                ),
                start_time=start_time,
                command=command,
                metadata={
                    "dataset_path": str(dataset_path),
                    "bounded_input_path": str(bounded_dataset_path),
                    "rows_copied": copied_rows,
                },
            )

        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        summary_payload: dict[str, Any] = {}
        if temp_summary_path.is_file():
            try:
                summary_payload = json.loads(temp_summary_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                summary_payload = {}

        status = "passed" if result.returncode == 0 else "failed"
        details = (
            f"Validated {copied_rows} dataset rows against the raw IoT contract."
            if result.returncode == 0
            else "Data contract validation reported issues."
        )
        return make_result(
            name="data_contract_validation",
            status=status,
            required=False,
            details=details,
            start_time=start_time,
            command=command,
            return_code=result.returncode,
            stdout_excerpt=shorten_output(result.stdout),
            stderr_excerpt=shorten_output(result.stderr),
            metadata={
                "dataset_path": str(dataset_path),
                "bounded_input_path": str(bounded_dataset_path),
                "rows_copied": copied_rows,
                "summary": summary_payload,
            },
        )
    finally:
        for temporary_path in (bounded_dataset_path, temp_summary_path):
            if temporary_path and temporary_path.exists():
                temporary_path.unlink(missing_ok=True)


def run_terraform_validation(*, skip_terraform: bool, dry_run: bool) -> CheckResult:
    start_time = time.time()
    if skip_terraform:
        return make_result(
            name="terraform_validation",
            status="skipped",
            required=False,
            details="Skipped because --skip-terraform was provided.",
            start_time=start_time,
        )

    if shutil.which("terraform") is None:
        return make_result(
            name="terraform_validation",
            status="failed",
            required=False,
            details="Terraform CLI was not found on PATH.",
            start_time=start_time,
        )

    commands = [
        ["terraform", "init", "-backend=false"],
        ["terraform", "validate"],
    ]

    if dry_run:
        return make_result(
            name="terraform_validation",
            status="dry_run",
            required=False,
            details=(
                "Would run Terraform init without backend state and then Terraform validate "
                "under infra/aws-orchestration. No terraform plan or apply is used."
            ),
            start_time=start_time,
            metadata={"commands": commands, "cwd": str(TERRAFORM_ROOT)},
        )

    combined_stdout: list[str] = []
    combined_stderr: list[str] = []
    environment = os.environ.copy()
    environment["TF_IN_AUTOMATION"] = "true"
    environment["TF_INPUT"] = "false"
    init_warning: str | None = None

    for index, command in enumerate(commands):
        result = subprocess.run(
            command,
            cwd=TERRAFORM_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            combined_stdout.append("$ " + " ".join(command))
            combined_stdout.append(result.stdout.strip())
        if result.stderr:
            combined_stderr.append("$ " + " ".join(command))
            combined_stderr.append(result.stderr.strip())
        if result.returncode != 0:
            stderr_lower = (result.stderr or "").lower()
            if index == 0 and (
                "failed to query available provider packages" in stderr_lower
                or "could not connect to registry.terraform.io" in stderr_lower
            ):
                init_warning = (
                    "terraform init -backend=false could not refresh provider metadata "
                    "because registry.terraform.io was unreachable, so the smoke test "
                    "continued with terraform validate."
                )
                continue
            return make_result(
                name="terraform_validation",
                status="failed",
                required=False,
                details="Terraform validation failed.",
                start_time=start_time,
                command=command,
                return_code=result.returncode,
                stdout_excerpt=shorten_output("\n".join(combined_stdout)),
                stderr_excerpt=shorten_output("\n".join(combined_stderr)),
            )

    return make_result(
        name="terraform_validation",
        status="passed",
        required=False,
        details=(
            init_warning
            if init_warning is not None
            else "Terraform init -backend=false and terraform validate succeeded."
        ),
        start_time=start_time,
        stdout_excerpt=shorten_output("\n".join(combined_stdout)),
        stderr_excerpt=shorten_output("\n".join(combined_stderr)),
        metadata={"cwd": str(TERRAFORM_ROOT)},
    )


def run_anomaly_detection_check(
    *,
    skip_anomaly_detection: bool,
    max_rows: int,
    dry_run: bool,
    check_name: str = "anomaly_detection_read_only",
) -> CheckResult:
    start_time = time.time()
    if skip_anomaly_detection:
        return make_result(
            name=check_name,
            status="skipped",
            required=False,
            details="Skipped because --skip-anomaly-detection was provided.",
            start_time=start_time,
        )

    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_anomaly_detection.py"),
        "--limit",
        str(max_rows),
        "--dry-run",
    ]

    if dry_run:
        return make_result(
            name=check_name,
            status="dry_run",
            required=False,
            details=(
                "Would attempt the anomaly detection helper in its own dry-run mode. "
                "This stays read-only and does not write anomaly rows."
            ),
            start_time=start_time,
            command=command,
        )

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    stderr_text = (result.stderr or "").lower()
    stdout_text = (result.stdout or "").lower()
    combined_text = stderr_text + "\n" + stdout_text
    if result.returncode != 0 and any(hint in combined_text for hint in MISSING_RUNTIME_HINTS):
        return make_result(
            name=check_name,
            status="skipped",
            required=False,
            details=(
                "Skipped because the local PostgreSQL/Docker runtime is not ready for a "
                "read-only anomaly detection probe."
            ),
            start_time=start_time,
            command=command,
            return_code=result.returncode,
            stdout_excerpt=shorten_output(result.stdout),
            stderr_excerpt=shorten_output(result.stderr),
        )

    status = "passed" if result.returncode == 0 else "failed"
    details = (
        "Read-only anomaly detection helper completed without database writes."
        if result.returncode == 0
        else "Anomaly detection helper returned a non-zero exit code."
    )
    return make_result(
        name=check_name,
        status=status,
        required=False,
        details=details,
        start_time=start_time,
        command=command,
        return_code=result.returncode,
        stdout_excerpt=shorten_output(result.stdout),
        stderr_excerpt=shorten_output(result.stderr),
    )


def summarize_results(results: list[CheckResult]) -> dict[str, Any]:
    counts: dict[str, int] = {
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "dry_run": 0,
    }
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    required_failures = [
        result.name for result in results if result.required and result.status == "failed"
    ]
    overall_status = "passed" if not required_failures else "failed"
    return {
        "overall_status": overall_status,
        "counts": counts,
        "required_failures": required_failures,
    }


def print_human_summary(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
    results: list[CheckResult],
    summary: dict[str, Any],
) -> None:
    print("Local E2E smoke test summary")
    print(f"Profile: {profile_name}")
    print(f"Dataset path: {dataset_path}")
    print(f"Max rows: {max_rows}")
    print(f"Overall status: {summary['overall_status']}")
    print("Checks:")
    for result in results:
        print(f"- {result.name}: {result.status} ({result.details})")


def build_output_payload(
    *,
    args: argparse.Namespace,
    dataset_path: Path,
    results: list[CheckResult],
) -> dict[str, Any]:
    summary = summarize_results(results)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "project_root": str(PROJECT_ROOT),
        "profile": args.profile,
        "dataset_path": str(dataset_path),
        "max_rows": args.max_rows,
        "dry_run": args.dry_run,
        "run_sample_pipeline": args.run_sample_pipeline,
        "safeguards": {
            "starts_full_pipeline": False,
            "runs_controlled_sample_pipeline_only_when_requested": True,
            "runs_full_dataset_by_default": False,
            "deploys_aws": False,
            "runs_terraform_apply": False,
        },
        "skips": {
            "airflow": args.skip_airflow,
            "dbt": args.skip_dbt,
            "anomaly_detection": args.skip_anomaly_detection,
            "terraform": args.skip_terraform,
        },
        "summary": summary,
        "checks": [asdict(result) for result in results],
    }


def write_output_json(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()

    try:
        ensure_args_are_valid(args)
        profiles = load_dataset_profiles(DATASET_PROFILES_PATH)
        selected_profile = profiles.get(args.profile)
        if selected_profile is None:
            raise SmokeTestConfigurationError(
                f"Profile '{args.profile}' was not found in {DATASET_PROFILES_PATH}."
            )

        expected_input_path = selected_profile.get("expected_input_path")
        if not isinstance(expected_input_path, str) or not expected_input_path:
            raise SmokeTestConfigurationError(
                f"Profile '{args.profile}' is missing expected_input_path."
            )
        dataset_path = PROJECT_ROOT / expected_input_path

        results = [
            check_expected_repository_structure(),
            check_selected_dataset(
                profile_name=args.profile,
                dataset_path=dataset_path,
                max_rows=args.max_rows,
            ),
            check_dbt_foundation(skip_dbt=args.skip_dbt),
            run_command(
                name="docker_compose_config",
                command=["docker", "compose", "config"],
                required=True,
                dry_run=args.dry_run,
                success_detail="docker compose config completed successfully.",
                dry_run_detail="Would run docker compose config without starting services.",
            ),
            check_python_syntax(skip_airflow=args.skip_airflow),
            run_terraform_validation(
                skip_terraform=args.skip_terraform,
                dry_run=args.dry_run,
            ),
        ]

        if dataset_path.is_file():
            results.append(
                run_data_contract_validation(
                    dataset_path=dataset_path,
                    max_rows=args.max_rows,
                    dry_run=args.dry_run,
                )
            )
        else:
            results.append(
                make_result(
                    name="data_contract_validation",
                    status="skipped",
                    required=False,
                    details="Skipped because the selected dataset file does not exist.",
                )
            )

        results.append(
            run_anomaly_detection_check(
                skip_anomaly_detection=args.skip_anomaly_detection,
                max_rows=args.max_rows,
                dry_run=args.dry_run,
            )
        )

        if args.run_sample_pipeline:
            results.extend(
                run_controlled_sample_pipeline(
                    dataset_path=dataset_path,
                    max_rows=args.max_rows,
                    dry_run=args.dry_run,
                    skip_anomaly_detection=args.skip_anomaly_detection,
                )
            )
        else:
            results.append(
                make_result(
                    name="sample_pipeline_runtime",
                    status="skipped",
                    required=False,
                    details="Skipped because --run-sample-pipeline was not provided.",
                )
            )

        payload = build_output_payload(
            args=args,
            dataset_path=dataset_path,
            results=results,
        )
        print_human_summary(
            profile_name=args.profile,
            dataset_path=dataset_path,
            max_rows=args.max_rows,
            results=results,
            summary=payload["summary"],
        )

        if args.output_json:
            write_output_json(PROJECT_ROOT / args.output_json, payload)
            print(f"JSON summary written to: {args.output_json}")

        return 0 if payload["summary"]["overall_status"] == "passed" else 1
    except SmokeTestConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"Input/output error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
