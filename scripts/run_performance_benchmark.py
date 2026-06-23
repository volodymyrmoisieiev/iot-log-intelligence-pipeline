#!/usr/bin/env python3
"""Run a lightweight local performance benchmark for the Docker-based pipeline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "docs" / "performance" / "results"
ALLOWED_PROFILES = ("sample", "medium", "full")


@dataclass
class BenchmarkStep:
    name: str
    command: list[str]


class BenchmarkError(RuntimeError):
    """Raised when benchmark configuration or execution fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a lightweight local benchmark for the Go producer, Python "
            "consumer, and warehouse loader."
        )
    )
    parser.add_argument(
        "--profile",
        choices=ALLOWED_PROFILES,
        default="sample",
        help="Dataset profile to benchmark. Default: sample.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=72,
        help="Row cap passed to the producer. Default: 72.",
    )
    parser.add_argument(
        "--consumer-messages",
        type=int,
        default=None,
        help="Message cap for the Python consumer. Default: same value as --rows.",
    )
    parser.add_argument(
        "--loader-messages",
        type=int,
        default=None,
        help="Message cap for the warehouse loader. Default: same value as --rows.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output file or directory for the benchmark JSON report. Default: "
            "docs/performance/results/."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands and write a report without executing Docker Compose.",
    )
    parser.add_argument(
        "--summary-md",
        default=None,
        help=(
            "Optional Markdown file or directory for a human-readable benchmark "
            "summary. Default: disabled."
        ),
    )
    return parser.parse_args()


def resolve_positive_int(value: int | None, fallback: int, flag_name: str) -> int:
    resolved = fallback if value is None else value
    if resolved <= 0:
        raise BenchmarkError(f"{flag_name} must be a positive integer.")
    return resolved


def resolve_output_path(output_value: str | None, profile: str, rows: int) -> Path:
    timestamp_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not output_value:
        return DEFAULT_OUTPUT_DIR / (
            f"benchmark_{profile}_{rows}_rows_{timestamp_token}.json"
        )

    output_path = Path(output_value)
    if not output_path.is_absolute():
        output_path = (REPO_ROOT / output_path).resolve()

    if output_path.suffix.lower() == ".json":
        return output_path

    return output_path / f"benchmark_{profile}_{rows}_rows_{timestamp_token}.json"


def resolve_summary_path(
    summary_value: str | None, profile: str, rows: int
) -> Path | None:
    if not summary_value:
        return None

    summary_path = Path(summary_value)
    if not summary_path.is_absolute():
        summary_path = (REPO_ROOT / summary_value).resolve()

    if summary_path.suffix.lower() == ".md":
        return summary_path

    timestamp_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return summary_path / f"benchmark_{profile}_{rows}_rows_{timestamp_token}.md"


def dataset_path_for_profile(profile: str) -> Path:
    if profile == "sample":
        return REPO_ROOT / "data" / "samples" / "sample_iot_logs.csv"
    if profile == "medium":
        return REPO_ROOT / "data" / "processed" / "medium_iot_logs.csv"
    return REPO_ROOT / "data" / "raw" / "full_iot_logs.csv"


def validate_profile_input(profile: str) -> None:
    dataset_path = dataset_path_for_profile(profile)
    if dataset_path.is_file():
        return

    if profile == "medium":
        command = (
            "python .\\scripts\\create_dataset_profile.py --input "
            ".\\data\\raw\\RT_IOT2022.csv --output "
            ".\\data\\processed\\medium_iot_logs.csv --rows 10000 --overwrite"
        )
        raise BenchmarkError(
            "Missing medium dataset file: "
            f"{dataset_path}\nGenerate it first with:\n{command}"
        )

    if profile == "full":
        raise BenchmarkError(
            "Missing full dataset file: "
            f"{dataset_path}\nThe full dataset is intentionally not committed to "
            "Git. Place it at data/raw/full_iot_logs.csv before running a full "
            "benchmark."
        )

    raise BenchmarkError(f"Missing sample dataset file: {dataset_path}")


def build_steps() -> list[BenchmarkStep]:
    return [
        BenchmarkStep(
            name="go-producer",
            command=["docker", "compose", "run", "--rm", "go-producer"],
        ),
        BenchmarkStep(
            name="python-consumer",
            command=["docker", "compose", "run", "--rm", "python-consumer"],
        ),
        BenchmarkStep(
            name="warehouse-loader",
            command=["docker", "compose", "run", "--rm", "warehouse-loader"],
        ),
    ]


def build_benchmark_env(
    profile: str, rows: int, consumer_messages: int, loader_messages: int
) -> dict[str, str]:
    progress_interval = max(1, min(max(consumer_messages, loader_messages), 1000))

    env = os.environ.copy()
    env.update(
        {
            "DATASET_PROFILE": profile,
            "PRODUCER_MAX_ROWS": str(rows),
            "PRODUCER_SEND_DELAY_MS": "0",
            "CONSUMER_MAX_MESSAGES": str(consumer_messages),
            "WAREHOUSE_LOADER_MAX_MESSAGES": str(loader_messages),
            "CONSUMER_PROGRESS_INTERVAL": str(
                max(1, min(consumer_messages, progress_interval))
            ),
            "WAREHOUSE_LOADER_PROGRESS_INTERVAL": str(
                max(1, min(loader_messages, progress_interval))
            ),
        }
    )
    return env


def print_header(
    profile: str,
    rows: int,
    consumer_messages: int,
    loader_messages: int,
    output_path: Path,
    dry_run: bool,
) -> None:
    print("Stage 16 performance benchmark")
    print(f"Repository root: {REPO_ROOT}")
    print(f"Profile: {profile}")
    print(f"Producer row cap: {rows}")
    print(f"Consumer message cap: {consumer_messages}")
    print(f"Warehouse loader message cap: {loader_messages}")
    print(f"Report path: {output_path}")
    print(f"Mode: {'dry-run' if dry_run else 'execute'}")
    print("Note: Kafka/PostgreSQL dependencies should already be running locally.")


def write_report(output_path: Path, report: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def format_return_code(return_code: Any) -> str:
    if return_code is None:
        return "not executed (dry-run)"
    return str(return_code)


def build_interpretation(report: dict[str, Any]) -> list[str]:
    if report["dry_run"]:
        return [
            "This was a dry run, so the benchmark did not execute Docker Compose commands.",
            "Elapsed times remain at 0 seconds and return codes are recorded as not executed.",
        ]

    failed_steps = [
        step["name"] for step in report["commands"] if step["return_code"] not in (0, None)
    ]
    if failed_steps:
        return [
            "At least one benchmark step failed, so compare the return codes before treating these timings as meaningful.",
            "Investigate the failed step first, then rerun the benchmark on the same profile and message caps.",
        ]

    return [
        "These timings show how long the local Docker-based producer, consumer, and loader steps took for this dataset profile.",
        "Use repeated runs on the same machine and Docker resource settings for fair comparisons between sample, medium, and full profiles.",
    ]


def build_markdown_summary(report: dict[str, Any]) -> str:
    rows = [
        "| Step | Command | Elapsed seconds | Return code |",
        "| --- | --- | ---: | --- |",
    ]
    for step in report["commands"]:
        rows.append(
            "| "
            f"{step['name']} | "
            f"`{subprocess.list2cmdline(step['command'])}` | "
            f"{step['elapsed_seconds']:.3f} | "
            f"{format_return_code(step['return_code'])} |"
        )

    interpretation_lines = "\n".join(
        f"- {line}" for line in build_interpretation(report)
    )

    return "\n".join(
        [
            "# Benchmark Summary",
            "",
            f"- Benchmark timestamp: `{report['timestamp']}`",
            f"- Dataset profile: `{report['profile']}`",
            f"- Producer rows: `{report['rows']}`",
            f"- Consumer messages: `{report['consumer_messages']}`",
            f"- Warehouse loader messages: `{report['loader_messages']}`",
            f"- Dry run: `{report['dry_run']}`",
            "",
            "## Step Results",
            "",
            *rows,
            "",
            f"**Total elapsed time:** `{report['total_elapsed_seconds']:.3f}` seconds",
            "",
            "## Interpretation",
            "",
            interpretation_lines,
            "",
            "## Note",
            "",
            "- Results depend on the local machine, active Docker resource limits, and any other workload running at the same time.",
            "- Treat these numbers as environment-specific local measurements rather than portable absolute performance claims.",
            "",
        ]
    )


def write_markdown_summary(summary_path: Path, report: dict[str, Any]) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(build_markdown_summary(report), encoding="utf-8")


def run_step(
    step: BenchmarkStep, step_index: int, total_steps: int, env: dict[str, str]
) -> tuple[float, int]:
    command_text = subprocess.list2cmdline(step.command)
    print()
    print(f"Step {step_index}/{total_steps}: {step.name}")
    print(f"Command: {command_text}")
    start = time.perf_counter()
    completed = subprocess.run(step.command, cwd=REPO_ROOT, env=env, check=False)
    elapsed = time.perf_counter() - start
    print(f"Return code: {completed.returncode}")
    print(f"Elapsed seconds: {elapsed:.3f}")
    return elapsed, completed.returncode


def dry_run_step(step: BenchmarkStep, step_index: int, total_steps: int) -> None:
    command_text = subprocess.list2cmdline(step.command)
    print()
    print(f"Step {step_index}/{total_steps}: {step.name}")
    print(f"Dry run command: {command_text}")


def main() -> int:
    args = parse_args()

    try:
        rows = resolve_positive_int(args.rows, 72, "--rows")
        consumer_messages = resolve_positive_int(
            args.consumer_messages, rows, "--consumer-messages"
        )
        loader_messages = resolve_positive_int(
            args.loader_messages, rows, "--loader-messages"
        )
        validate_profile_input(args.profile)
        output_path = resolve_output_path(args.output, args.profile, rows)
        summary_path = resolve_summary_path(args.summary_md, args.profile, rows)
        steps = build_steps()
        benchmark_env = build_benchmark_env(
            args.profile,
            rows,
            consumer_messages,
            loader_messages,
        )
        timestamp = datetime.now(timezone.utc).isoformat()

        print_header(
            profile=args.profile,
            rows=rows,
            consumer_messages=consumer_messages,
            loader_messages=loader_messages,
            output_path=output_path,
            dry_run=args.dry_run,
        )

        step_reports: list[dict[str, Any]] = []
        total_elapsed = 0.0

        for step_index, step in enumerate(steps, start=1):
            if args.dry_run:
                dry_run_step(step, step_index, len(steps))
                step_reports.append(
                    {
                        "name": step.name,
                        "command": step.command,
                        "elapsed_seconds": 0.0,
                        "return_code": None,
                    }
                )
                continue

            elapsed, return_code = run_step(
                step=step,
                step_index=step_index,
                total_steps=len(steps),
                env=benchmark_env,
            )
            total_elapsed += elapsed
            step_reports.append(
                {
                    "name": step.name,
                    "command": step.command,
                    "elapsed_seconds": round(elapsed, 6),
                    "return_code": return_code,
                }
            )
            if return_code != 0:
                raise BenchmarkError(
                    f"Benchmark step '{step.name}' failed with return code "
                    f"{return_code}."
                )

        report = {
            "timestamp": timestamp,
            "profile": args.profile,
            "rows": rows,
            "consumer_messages": consumer_messages,
            "loader_messages": loader_messages,
            "dry_run": args.dry_run,
            "commands": step_reports,
            "total_elapsed_seconds": round(total_elapsed, 6),
        }
        write_report(output_path, report)
        if summary_path is not None:
            write_markdown_summary(summary_path, report)

        print()
        print("Benchmark summary")
        print(f"Steps recorded: {len(step_reports)}")
        print(f"Total elapsed seconds: {total_elapsed:.3f}")
        print(f"JSON report written to: {output_path}")
        if summary_path is not None:
            print(f"Markdown summary written to: {summary_path}")
        return 0
    except BenchmarkError as exc:
        print(f"Benchmark error: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"OS error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
