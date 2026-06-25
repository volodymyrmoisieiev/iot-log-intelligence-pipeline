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
DEFAULT_PROGRESS_INTERVAL = 1000
DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE = 1000
KAFKA_TOPIC_TOOL_PATH = "/opt/kafka/bin/kafka-topics.sh"
PROGRESS_MODE_CHOICES = ("auto", "log", "tqdm", "bar")

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


@dataclass(frozen=True)
class CommandExecutionResult:
    return_code: int
    stdout_text: str | None
    stderr_text: str | None
    streamed_output: bool


@dataclass(frozen=True)
class ConcurrentProcessSpec:
    name: str
    command: list[str]
    cwd: Path
    env: dict[str, str] | None = None


@dataclass
class ConcurrentProcessHandle:
    spec: ConcurrentProcessSpec
    process: subprocess.Popen[str]
    start_time: float
    stream_output: bool
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    stdout_handle: Any | None = None
    stderr_handle: Any | None = None
    terminated_by_orchestrator: bool = False
    termination_reason: str | None = None


@dataclass(frozen=True)
class ConcurrentProcessResult:
    name: str
    return_code: int | None
    duration_seconds: float
    stdout_excerpt: str | None
    stderr_excerpt: str | None
    terminated_by_orchestrator: bool
    termination_reason: str | None


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
        "--run-profile-pipeline",
        action="store_true",
        help=(
            "Run a controlled profile-specific producer/consumer/warehouse-loader flow "
            "with bounded row limits."
        ),
    )
    parser.add_argument(
        "--run-sample-pipeline",
        action="store_true",
        help=(
            "Backward-compatible alias for --run-profile-pipeline when --profile sample is used."
        ),
    )
    parser.add_argument(
        "--concurrent-pipeline",
        action="store_true",
        help=(
            "Prepare for a future concurrent profile pipeline mode where consumer, "
            "warehouse-loader, and producer run at the same time."
        ),
    )
    parser.add_argument(
        "--allow-full-run",
        action="store_true",
        help="Explicitly allow a controlled full-profile runtime flow.",
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
    parser.add_argument(
        "--stream-output",
        action="store_true",
        help="Stream subprocess stdout/stderr live to the terminal during manual runs.",
    )
    parser.add_argument(
        "--progress-mode",
        choices=PROGRESS_MODE_CHOICES,
        default="auto",
        help="Progress display mode for Python pipeline components. Default: auto.",
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
            "--run-sample-pipeline is a backward-compatible alias for the sample profile only. "
            "Use --run-profile-pipeline for medium or future profile runtime checks."
        )
    if args.concurrent_pipeline and not args.run_profile_pipeline:
        raise SmokeTestConfigurationError(
            "--concurrent-pipeline requires --run-profile-pipeline."
        )
    if args.run_profile_pipeline and args.profile == "full" and not args.allow_full_run:
        raise SmokeTestConfigurationError(
            "Refusing --profile full --run-profile-pipeline without --allow-full-run. "
            "Full or 100k-style validation is reserved for later Stage 21D work."
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


def read_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def is_docker_permission_issue(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(hint in lowered for hint in DOCKER_PERMISSION_HINTS)


def count_bounded_dataset_rows(dataset_path: Path, max_rows: int) -> int:
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


def count_total_dataset_rows(dataset_path: Path) -> int:
    with dataset_path.open("r", newline="", encoding="utf-8-sig") as source_file:
        reader = csv.reader(source_file)
        header = next(reader, None)
        if header is None:
            raise SmokeTestConfigurationError(
                f"Dataset file is missing a header row: {dataset_path}"
            )

        return sum(1 for _ in reader)


def build_missing_dataset_instruction(profile_name: str) -> str:
    if profile_name == "medium":
        return (
            "Generate it first with: python .\\scripts\\create_dataset_profile.py "
            "--input .\\data\\raw\\RT_IOT2022.csv --output .\\data\\processed\\medium_iot_logs.csv "
            "--rows 10000 --overwrite"
        )
    if profile_name == "full":
        return (
            "Place the full dataset at data/raw/full_iot_logs.csv and use --allow-full-run "
            "only for an intentional full-profile runtime check."
        )
    return "Restore or provide the selected dataset file before running the smoke test."


def check_dataset_preflight(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
) -> CheckResult:
    start_time = time.time()
    metadata: dict[str, Any] = {
        "profile": profile_name,
        "resolved_dataset_path": str(dataset_path),
        "dataset_exists": dataset_path.is_file(),
        "requested_max_rows": max_rows,
    }

    if not dataset_path.is_file():
        return make_result(
            name="dataset_preflight",
            status="failed",
            required=True,
            details=(
                f"Resolved dataset path for profile '{profile_name}' is missing: {dataset_path}. "
                + build_missing_dataset_instruction(profile_name)
            ),
            start_time=start_time,
            metadata=metadata,
        )

    available_rows = count_total_dataset_rows(dataset_path)
    metadata["available_rows"] = available_rows
    metadata["bounded_rows_for_checks"] = min(available_rows, max_rows)
    if profile_name == "full" and available_rows < max_rows:
        return make_result(
            name="dataset_preflight",
            status="failed",
            required=True,
            details=(
                f"Resolved full dataset path exists, but only {available_rows} rows are available while "
                f"{max_rows} rows were requested. Provide a larger full dataset at {dataset_path} or lower --max-rows."
            ),
            start_time=start_time,
            metadata=metadata,
        )
    details = (
        f"Resolved dataset path: {dataset_path}. Exists: yes. "
        f"Available rows: {available_rows}. "
        f"Bounded rows for checks/runtime: {min(available_rows, max_rows)}."
    )
    return make_result(
        name="dataset_preflight",
        status="passed",
        required=True,
        details=details,
        start_time=start_time,
        metadata=metadata,
    )


def build_runtime_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}-{os.getpid()}"


def choose_progress_interval(expected_rows: int) -> int:
    return max(1, min(DEFAULT_PROGRESS_INTERVAL, expected_rows))


def determine_pipeline_execution_mode(*, concurrent_pipeline: bool) -> str:
    return "concurrent" if concurrent_pipeline else "sequential"


def create_pipeline_runtime_context(
    profile_name: str,
    max_rows: int,
    *,
    pipeline_execution_mode: str,
) -> dict[str, Any]:
    runtime_id = build_runtime_id(f"stage21c-{profile_name}")
    topic_suffix = runtime_id.replace("-", "_")
    raw_topic = f"iot_raw_logs_{topic_suffix}"
    processed_topic = f"iot_processed_logs_{topic_suffix}"
    invalid_topic = f"iot_invalid_logs_{topic_suffix}"
    progress_interval = choose_progress_interval(max_rows)
    return {
        "runtime_id": runtime_id,
        "profile": profile_name,
        "pipeline_execution_mode": pipeline_execution_mode,
        "raw_topic": raw_topic,
        "processed_topic": processed_topic,
        "invalid_topic": invalid_topic,
        "consumer_group_id": f"{runtime_id}-consumer",
        "loader_group_id": f"{runtime_id}-loader",
        "max_rows": max_rows,
        "producer_progress_interval": progress_interval,
        "producer_progress_mode": "log",
        "consumer_progress_interval": progress_interval,
        "warehouse_loader_progress_interval": progress_interval,
        "warehouse_loader_batch_size": DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE,
        "python_progress_mode": "tqdm_if_tty_else_log",
    }


def normalize_progress_mode(progress_mode: str) -> str:
    if progress_mode == "auto":
        return "tqdm_if_tty_else_log"
    return progress_mode


def resolve_python_progress_mode(*, stream_output: bool, progress_mode: str) -> str:
    if progress_mode == "bar":
        return "bar" if stream_output else "log"
    if stream_output and progress_mode == "tqdm":
        return "bar"
    return progress_mode


def choose_producer_progress_mode(*, stream_output: bool, progress_mode: str) -> str:
    if stream_output and progress_mode == "bar":
        return "bar"
    return "log"


def run_process(
    *,
    command: list[str],
    cwd: Path,
    env: dict[str, str] | None,
    stream_output: bool,
) -> CommandExecutionResult:
    if stream_output:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            check=False,
        )
        return CommandExecutionResult(
            return_code=result.returncode,
            stdout_text=None,
            stderr_text=None,
            streamed_output=True,
        )

    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandExecutionResult(
        return_code=result.returncode,
        stdout_text=result.stdout,
        stderr_text=result.stderr,
        streamed_output=False,
    )


def start_concurrent_process(
    spec: ConcurrentProcessSpec,
    *,
    stream_output: bool,
) -> ConcurrentProcessHandle:
    stdout_handle: Any | None = None
    stderr_handle: Any | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None

    if not stream_output:
        stdout_file = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
        stderr_file = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
        stdout_path = Path(stdout_file.name)
        stderr_path = Path(stderr_file.name)
        stdout_file.close()
        stderr_file.close()
        stdout_handle = stdout_path.open("w", encoding="utf-8")
        stderr_handle = stderr_path.open("w", encoding="utf-8")

    try:
        process = subprocess.Popen(
            spec.command,
            cwd=spec.cwd,
            env=spec.env,
            stdout=None if stream_output else stdout_handle,
            stderr=None if stream_output else stderr_handle,
            text=True,
        )
    except Exception:
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        if stdout_path is not None:
            stdout_path.unlink(missing_ok=True)
        if stderr_path is not None:
            stderr_path.unlink(missing_ok=True)
        raise

    return ConcurrentProcessHandle(
        spec=spec,
        process=process,
        start_time=time.time(),
        stream_output=stream_output,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
    )


def terminate_concurrent_processes(
    handles: list[ConcurrentProcessHandle],
    *,
    reason: str,
    wait_seconds: float = 3.0,
) -> None:
    running_handles = [handle for handle in handles if handle.process.poll() is None]
    if not running_handles:
        return

    for handle in running_handles:
        handle.terminated_by_orchestrator = True
        handle.termination_reason = reason
        try:
            handle.process.terminate()
        except ProcessLookupError:
            continue

    deadline = time.monotonic() + wait_seconds
    for handle in running_handles:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            handle.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                handle.process.kill()
                handle.process.wait(timeout=1.0)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                continue


def collect_concurrent_process_result(
    handle: ConcurrentProcessHandle,
) -> ConcurrentProcessResult:
    if handle.process.poll() is None:
        handle.process.wait()

    if handle.stdout_handle is not None and not handle.stdout_handle.closed:
        handle.stdout_handle.close()
    if handle.stderr_handle is not None and not handle.stderr_handle.closed:
        handle.stderr_handle.close()

    stdout_text: str | None
    stderr_text: str | None
    if handle.stream_output:
        stdout_text = "streamed to terminal"
        stderr_text = "streamed to terminal"
    else:
        stdout_text = read_text_if_exists(handle.stdout_path) if handle.stdout_path else None
        stderr_text = read_text_if_exists(handle.stderr_path) if handle.stderr_path else None

    if handle.stdout_path is not None:
        handle.stdout_path.unlink(missing_ok=True)
    if handle.stderr_path is not None:
        handle.stderr_path.unlink(missing_ok=True)

    return ConcurrentProcessResult(
        name=handle.spec.name,
        return_code=handle.process.returncode,
        duration_seconds=round(time.time() - handle.start_time, 3),
        stdout_excerpt=shorten_output(stdout_text),
        stderr_excerpt=shorten_output(stderr_text),
        terminated_by_orchestrator=handle.terminated_by_orchestrator,
        termination_reason=handle.termination_reason,
    )


def wait_for_concurrent_processes(
    handles: list[ConcurrentProcessHandle],
    *,
    required_process_names: set[str] | None = None,
    poll_interval_seconds: float = 0.2,
    termination_wait_seconds: float = 3.0,
) -> list[ConcurrentProcessResult]:
    if required_process_names is None:
        required_process_names = {handle.spec.name for handle in handles}

    completed_process_names: set[str] = set()
    failure_reason: str | None = None

    while len(completed_process_names) < len(handles):
        for handle in handles:
            if handle.spec.name in completed_process_names:
                continue

            return_code = handle.process.poll()
            if return_code is None:
                continue

            completed_process_names.add(handle.spec.name)
            if return_code != 0 and handle.spec.name in required_process_names:
                failure_reason = (
                    f"terminated because required process '{handle.spec.name}' exited "
                    f"with code {return_code}"
                )
                terminate_concurrent_processes(
                    handles,
                    reason=failure_reason,
                    wait_seconds=termination_wait_seconds,
                )
                break

        if failure_reason is not None:
            break

        time.sleep(poll_interval_seconds)

    return [collect_concurrent_process_result(handle) for handle in handles]


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
    stream_output: bool = False,
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
            result = run_process(
                command=command,
                cwd=cwd,
                env=env,
                stream_output=stream_output,
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

        if result.streamed_output:
            combined_stdout = ["streamed to terminal"]
            combined_stderr = ["streamed to terminal"]
        elif result.stdout_text:
            combined_stdout.append("$ " + " ".join(command))
            combined_stdout.append(result.stdout_text.strip())
        if not result.streamed_output and result.stderr_text:
            combined_stderr.append("$ " + " ".join(command))
            combined_stderr.append(result.stderr_text.strip())

        if result.return_code != 0:
            return make_result(
                name=name,
                status="failed",
                required=required,
                details="Command sequence returned a non-zero exit code.",
                start_time=start_time,
                command=command,
                return_code=result.return_code,
                stdout_excerpt=shorten_output("\n".join(combined_stdout)) if combined_stdout else None,
                stderr_excerpt=shorten_output("\n".join(combined_stderr)) if combined_stderr else None,
                metadata=metadata,
            )

    return make_result(
        name=name,
        status="passed",
        required=required,
        details=success_detail,
        start_time=start_time,
        stdout_excerpt=shorten_output("\n".join(combined_stdout)) if combined_stdout else None,
        stderr_excerpt=shorten_output("\n".join(combined_stderr)) if combined_stderr else None,
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
                name="profile_pipeline_postgres_counts",
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
                name="profile_pipeline_postgres_counts",
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
                name="profile_pipeline_postgres_counts",
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
                name="profile_pipeline_postgres_counts",
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
                name="profile_pipeline_postgres_counts",
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
            name="profile_pipeline_postgres_counts",
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


def run_concurrent_profile_pipeline(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
    dry_run: bool,
    skip_anomaly_detection: bool,
    stream_output: bool,
    progress_mode: str,
) -> list[CheckResult]:
    # Stage 23C adds reusable Popen-based concurrent process helpers,
    # but Stage 23D will wire those helpers into the real producer,
    # consumer, and warehouse-loader runtime orchestration path.
    expected_rows = count_bounded_dataset_rows(dataset_path=dataset_path, max_rows=max_rows)
    context = create_pipeline_runtime_context(
        profile_name=profile_name,
        max_rows=max_rows,
        pipeline_execution_mode="concurrent",
    )
    progress_interval = choose_progress_interval(expected_rows)
    context["resolved_dataset_path"] = str(dataset_path)
    context["expected_dataset_rows"] = expected_rows
    context["producer_progress_interval"] = progress_interval
    context["consumer_progress_interval"] = progress_interval
    context["warehouse_loader_progress_interval"] = progress_interval
    context["warehouse_loader_batch_size"] = DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE
    effective_python_progress_mode = resolve_python_progress_mode(
        stream_output=stream_output,
        progress_mode=progress_mode,
    )
    context["python_progress_mode"] = normalize_progress_mode(effective_python_progress_mode)
    context["producer_progress_mode"] = choose_producer_progress_mode(
        stream_output=stream_output,
        progress_mode=effective_python_progress_mode,
    )
    context["concurrent_pipeline_implemented"] = False

    status = "dry_run" if dry_run else "failed"
    details = (
        f"Would prepare the controlled {profile_name} profile pipeline in concurrent mode, "
        "but Stage 23D will wire the actual parallel process orchestration into the runtime flow."
        if dry_run
        else (
            f"Concurrent profile pipeline mode was requested for the controlled {profile_name} "
            "runtime flow, but Stage 23D has not wired the actual parallel process "
            "orchestration into the producer/consumer/warehouse-loader path yet."
        )
    )
    return [
        make_result(
            name="profile_pipeline_concurrent_runtime",
            status=status,
            required=True,
            details=details,
            metadata=context,
        )
    ]


def run_sequential_profile_pipeline(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
    dry_run: bool,
    skip_anomaly_detection: bool,
    stream_output: bool,
    progress_mode: str,
) -> list[CheckResult]:
    expected_rows = count_bounded_dataset_rows(dataset_path=dataset_path, max_rows=max_rows)
    context = create_pipeline_runtime_context(
        profile_name=profile_name,
        max_rows=max_rows,
        pipeline_execution_mode="sequential",
    )
    progress_interval = choose_progress_interval(expected_rows)
    context["resolved_dataset_path"] = str(dataset_path)
    context["expected_dataset_rows"] = expected_rows
    context["producer_progress_interval"] = progress_interval
    context["consumer_progress_interval"] = progress_interval
    context["warehouse_loader_progress_interval"] = progress_interval
    context["warehouse_loader_batch_size"] = DEFAULT_WAREHOUSE_LOADER_BATCH_SIZE
    effective_python_progress_mode = resolve_python_progress_mode(
        stream_output=stream_output,
        progress_mode=progress_mode,
    )
    context["python_progress_mode"] = normalize_progress_mode(effective_python_progress_mode)
    context["producer_progress_mode"] = choose_producer_progress_mode(
        stream_output=stream_output,
        progress_mode=effective_python_progress_mode,
    )

    if expected_rows == 0:
        return [
            make_result(
                name="profile_pipeline_runtime",
                status="failed",
                required=True,
                details=(
                    f"The selected {profile_name} dataset has no data rows to exercise in runtime mode."
                ),
                metadata=context,
            )
        ]

    results: list[CheckResult] = []
    results.append(
        run_command(
            name="profile_pipeline_start_services",
            command=["docker", "compose", "up", "-d", "kafka", "kafka-init", "postgres"],
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Started required Docker Compose services for the controlled {profile_name} runtime flow."
            ),
            dry_run_detail=(
                f"Would start kafka, kafka-init, and postgres for the controlled {profile_name} runtime flow."
            ),
            stream_output=stream_output,
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
            name="profile_pipeline_create_topics",
            commands=topic_commands,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Created isolated Kafka topics for the controlled {profile_name} runtime flow."
            ),
            dry_run_detail=(
                f"Would create isolated Kafka topics for raw, processed, and invalid {profile_name} runtime messages."
            ),
            stream_output=stream_output,
            metadata={
                **context,
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
            name="profile_pipeline_pre_run_postgres_counts",
            status="dry_run",
            required=True,
            details=(
                f"Would capture PostgreSQL row counts before running the controlled {profile_name} pipeline."
            ),
            metadata=context,
        ),
        None,
    )
    pre_counts_result.name = "profile_pipeline_pre_run_postgres_counts"
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
        f"DATASET_PROFILE={profile_name}",
        "-e",
        f"KAFKA_RAW_TOPIC={context['raw_topic']}",
        "-e",
        f"PRODUCER_MAX_ROWS={expected_rows}",
        "-e",
        f"PRODUCER_PROGRESS_INTERVAL={context['producer_progress_interval']}",
        "-e",
        f"PRODUCER_PROGRESS_MODE={context['producer_progress_mode']}",
        "-e",
        "PRODUCER_SEND_DELAY_MS=0",
        "go-producer",
    ]
    results.append(
        run_command(
            name="profile_pipeline_producer",
            command=producer_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the Go producer against the {profile_name} profile with a row limit of {expected_rows}."
            ),
            dry_run_detail=(
                f"Would run the Go producer with DATASET_PROFILE={profile_name}, PRODUCER_MAX_ROWS={expected_rows}, "
                f"PRODUCER_PROGRESS_INTERVAL={context['producer_progress_interval']}, "
                f"PRODUCER_PROGRESS_MODE={context['producer_progress_mode']}, and PRODUCER_SEND_DELAY_MS=0."
            ),
            stream_output=stream_output,
            metadata={
                **context,
                "component": "producer",
                "progress_interval": context["producer_progress_interval"],
                "progress_mode": context["producer_progress_mode"],
                "output_mode": "streamed" if stream_output else "captured",
            },
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
        f"CONSUMER_PROGRESS_INTERVAL={context['consumer_progress_interval']}",
        "-e",
        f"PYTHON_PROGRESS_MODE={effective_python_progress_mode}",
        "python-consumer",
    ]
    results.append(
        run_command(
            name="profile_pipeline_consumer",
            command=consumer_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the Python consumer with CONSUMER_MAX_MESSAGES={expected_rows} on the isolated {profile_name} topics."
            ),
            dry_run_detail=(
                f"Would run the Python consumer with CONSUMER_MAX_MESSAGES={expected_rows}, "
                f"CONSUMER_PROGRESS_INTERVAL={context['consumer_progress_interval']}, "
                f"PYTHON_PROGRESS_MODE={effective_python_progress_mode}, and isolated Kafka topics."
            ),
            stream_output=stream_output,
            stream_output_to_temp=not stream_output,
            metadata={
                **context,
                "component": "consumer",
                "progress_interval": context["consumer_progress_interval"],
                "progress_mode": normalize_progress_mode(effective_python_progress_mode),
                "output_mode": "streamed" if stream_output else "captured",
            },
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
        f"WAREHOUSE_LOADER_PROGRESS_INTERVAL={context['warehouse_loader_progress_interval']}",
        "-e",
        f"WAREHOUSE_LOADER_BATCH_SIZE={context['warehouse_loader_batch_size']}",
        "-e",
        f"PYTHON_PROGRESS_MODE={effective_python_progress_mode}",
        "warehouse-loader",
    ]
    results.append(
        run_command(
            name="profile_pipeline_warehouse_loader",
            command=loader_command,
            required=True,
            dry_run=dry_run,
            success_detail=(
                f"Ran the warehouse loader with WAREHOUSE_LOADER_MAX_MESSAGES={expected_rows} on the isolated {profile_name} topics."
            ),
            dry_run_detail=(
                f"Would run the warehouse loader with WAREHOUSE_LOADER_MAX_MESSAGES={expected_rows}, "
                f"WAREHOUSE_LOADER_PROGRESS_INTERVAL={context['warehouse_loader_progress_interval']}, "
                f"WAREHOUSE_LOADER_BATCH_SIZE={context['warehouse_loader_batch_size']}, "
                f"PYTHON_PROGRESS_MODE={effective_python_progress_mode}, and isolated Kafka topics."
            ),
            stream_output=stream_output,
            stream_output_to_temp=not stream_output,
            metadata={
                **context,
                "component": "warehouse_loader",
                "progress_interval": context["warehouse_loader_progress_interval"],
                "batch_size": context["warehouse_loader_batch_size"],
                "progress_mode": normalize_progress_mode(effective_python_progress_mode),
                "output_mode": "streamed" if stream_output else "captured",
            },
        )
    )
    if not dry_run and results[-1].status == "failed":
        return results

    if dry_run:
        results.append(
            make_result(
                name="profile_pipeline_verify_postgres_deltas",
                status="dry_run",
                required=True,
                details=(
                    f"Would compare PostgreSQL row-count deltas after the controlled {profile_name} runtime flow."
                ),
                metadata=context,
            )
        )
    else:
        post_counts_result, post_counts = read_postgres_counts()
        post_counts_result.name = "profile_pipeline_post_run_postgres_counts"
        results.append(post_counts_result)
        if results[-1].status == "failed":
            return results

        if pre_counts is not None and post_counts is not None:
            processed_delta = post_counts["processed_count"] - pre_counts["processed_count"]
            invalid_delta = post_counts["invalid_count"] - pre_counts["invalid_count"]
            total_delta = processed_delta + invalid_delta
            details = (
                f"PostgreSQL row-count delta after the controlled {profile_name} runtime flow: "
                f"processed_delta={processed_delta}, invalid_delta={invalid_delta}, total_delta={total_delta}, "
                f"expected_rows={expected_rows}."
            )
            status = (
                "passed"
                if total_delta == expected_rows and processed_delta >= 0 and invalid_delta >= 0
                else "failed"
            )
            if total_delta != expected_rows:
                details = (
                    f"Controlled {profile_name} runtime loaded {total_delta} total rows into PostgreSQL, "
                    f"but expected {expected_rows} rows from the bounded {profile_name} dataset."
                )
            results.append(
                make_result(
                    name="profile_pipeline_verify_postgres_deltas",
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
        check_name="profile_pipeline_anomaly_detection_read_only",
    )
    results.append(anomaly_result)
    return results


def run_controlled_profile_pipeline(
    *,
    profile_name: str,
    dataset_path: Path,
    max_rows: int,
    dry_run: bool,
    skip_anomaly_detection: bool,
    stream_output: bool,
    progress_mode: str,
    pipeline_execution_mode: str,
) -> list[CheckResult]:
    if pipeline_execution_mode == "concurrent":
        return run_concurrent_profile_pipeline(
            profile_name=profile_name,
            dataset_path=dataset_path,
            max_rows=max_rows,
            dry_run=dry_run,
            skip_anomaly_detection=skip_anomaly_detection,
            stream_output=stream_output,
            progress_mode=progress_mode,
        )

    return run_sequential_profile_pipeline(
        profile_name=profile_name,
        dataset_path=dataset_path,
        max_rows=max_rows,
        dry_run=dry_run,
        skip_anomaly_detection=skip_anomaly_detection,
        stream_output=stream_output,
        progress_mode=progress_mode,
    )


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
    stream_output_to_temp: bool = False,
    stream_output: bool = False,
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
            command=command,
            metadata=metadata,
        )

    try:
        if stream_output:
            result = run_process(
                command=command,
                cwd=cwd,
                env=env,
                stream_output=True,
            )
            stdout_text = "streamed to terminal"
            stderr_text = "streamed to terminal"
        elif stream_output_to_temp:
            stdout_file = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
            stderr_file = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
            stdout_path = Path(stdout_file.name)
            stderr_path = Path(stderr_file.name)
            stdout_file.close()
            stderr_file.close()
            try:
                with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
                    "w", encoding="utf-8"
                ) as stderr_handle:
                    result = subprocess.run(command, cwd=cwd, env=env, stdout=stdout_handle, stderr=stderr_handle, text=True, check=False)
                stdout_text = read_text_if_exists(stdout_path)
                stderr_text = read_text_if_exists(stderr_path)
            finally:
                stdout_path.unlink(missing_ok=True)
                stderr_path.unlink(missing_ok=True)
        else:
            result = run_process(
                command=command,
                cwd=cwd,
                env=env,
                stream_output=False,
            )
            stdout_text = result.stdout_text
            stderr_text = result.stderr_text
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

    return_code = result.returncode if stream_output_to_temp else result.return_code
    status = "passed" if return_code == 0 else "failed"
    details = success_detail if return_code == 0 else "Command returned a non-zero exit code."
    return make_result(
        name=name,
        status=status,
        required=required,
        details=details,
        start_time=start_time,
        command=command,
        return_code=return_code,
        stdout_excerpt=shorten_output(stdout_text),
        stderr_excerpt=shorten_output(stderr_text),
        metadata=metadata,
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


def extract_stage_durations(results: list[CheckResult], run_profile_pipeline: bool) -> dict[str, float]:
    duration_by_name = {
        result.name: result.duration_seconds for result in results if result.duration_seconds
    }
    stage_durations: dict[str, float] = {}

    data_contract_duration = duration_by_name.get("data_contract_validation")
    if data_contract_duration is not None:
        stage_durations["data_contract_validation"] = data_contract_duration

    if run_profile_pipeline:
        for stage_name, result_name in (
            ("producer", "profile_pipeline_producer"),
            ("consumer", "profile_pipeline_consumer"),
            ("warehouse_loader", "profile_pipeline_warehouse_loader"),
            ("anomaly_detection", "profile_pipeline_anomaly_detection_read_only"),
        ):
            duration = duration_by_name.get(result_name)
            if duration is not None:
                stage_durations[stage_name] = duration
        postgres_duration = duration_by_name.get("profile_pipeline_verify_postgres_deltas")
        if postgres_duration is None:
            postgres_duration = duration_by_name.get("profile_pipeline_post_run_postgres_counts")
        if postgres_duration is not None:
            stage_durations["postgresql_verification"] = postgres_duration
    else:
        anomaly_duration = duration_by_name.get("anomaly_detection_read_only")
        if anomaly_duration is not None:
            stage_durations["anomaly_detection"] = anomaly_duration

    return stage_durations


def extract_profile_pipeline_progress(results: list[CheckResult]) -> dict[str, Any] | None:
    progress: dict[str, Any] = {}

    for result in results:
        if result.name == "profile_pipeline_producer" and result.metadata:
            progress["producer"] = {
                "progress_interval": result.metadata.get("progress_interval"),
                "mode": result.metadata.get("progress_mode"),
            }
        elif result.name == "profile_pipeline_consumer" and result.metadata:
            progress["consumer"] = {
                "progress_interval": result.metadata.get("progress_interval"),
                "mode": result.metadata.get("python_progress_mode"),
            }
        elif result.name == "profile_pipeline_warehouse_loader" and result.metadata:
            progress["warehouse_loader"] = {
                "progress_interval": result.metadata.get("progress_interval"),
                "batch_size": result.metadata.get("batch_size"),
                "mode": result.metadata.get("python_progress_mode"),
            }

    return progress or None


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
    pipeline_execution_mode = determine_pipeline_execution_mode(
        concurrent_pipeline=args.concurrent_pipeline
    )
    summary = summarize_results(results)
    stage_durations = extract_stage_durations(
        results=results,
        run_profile_pipeline=args.run_profile_pipeline,
    )
    profile_pipeline_progress = extract_profile_pipeline_progress(results)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "project_root": str(PROJECT_ROOT),
        "profile": args.profile,
        "dataset_path": str(dataset_path),
        "max_rows": args.max_rows,
        "dry_run": args.dry_run,
        "stream_output": args.stream_output,
        "pipeline_execution_mode": pipeline_execution_mode,
        "progress_mode": normalize_progress_mode(
            resolve_python_progress_mode(
                stream_output=args.stream_output,
                progress_mode=args.progress_mode,
            )
        ),
        "run_profile_pipeline": args.run_profile_pipeline,
        "run_sample_pipeline": args.run_sample_pipeline,
        "concurrent_pipeline": args.concurrent_pipeline,
        "allow_full_run": args.allow_full_run,
        "safeguards": {
            "starts_full_pipeline": False,
            "runs_controlled_profile_pipeline_only_when_requested": True,
            "runs_full_dataset_by_default": False,
            "requires_allow_full_run_for_full_profile_runtime": True,
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
        "stage_durations_seconds": stage_durations,
        "profile_pipeline_progress": profile_pipeline_progress,
        "checks": [asdict(result) for result in results],
    }


def write_output_json(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.run_sample_pipeline:
        args.run_profile_pipeline = True

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
        pipeline_execution_mode = determine_pipeline_execution_mode(
            concurrent_pipeline=args.concurrent_pipeline
        )

        results = [
            check_expected_repository_structure(),
            check_selected_dataset(
                profile_name=args.profile,
                dataset_path=dataset_path,
                max_rows=args.max_rows,
            ),
            check_dataset_preflight(
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
                stream_output=args.stream_output,
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

        if args.run_profile_pipeline and dataset_path.is_file():
            results.extend(
                run_controlled_profile_pipeline(
                    profile_name=args.profile,
                    dataset_path=dataset_path,
                    max_rows=args.max_rows,
                    dry_run=args.dry_run,
                    skip_anomaly_detection=args.skip_anomaly_detection,
                    stream_output=args.stream_output,
                    progress_mode=args.progress_mode,
                    pipeline_execution_mode=pipeline_execution_mode,
                )
            )
        elif args.run_profile_pipeline:
            results.append(
                make_result(
                    name="profile_pipeline_runtime",
                    status="failed",
                    required=True,
                    details=(
                        f"Cannot run the controlled {args.profile} profile pipeline because the "
                        f"resolved dataset file is missing. {build_missing_dataset_instruction(args.profile)}"
                    ),
                )
            )
        else:
            results.append(
                make_result(
                    name="profile_pipeline_runtime",
                    status="skipped",
                    required=False,
                    details="Skipped because neither --run-profile-pipeline nor --run-sample-pipeline was provided.",
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
