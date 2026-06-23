#!/usr/bin/env python3
"""Analyze local benchmark JSON files produced by run_performance_benchmark.py."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = REPO_ROOT / "docs" / "performance" / "results"
ALLOWED_PROFILES = ("sample", "medium", "full")
DEFAULT_STEP_ORDER = ("go-producer", "python-consumer", "warehouse-loader")


@dataclass
class StepResult:
    name: str
    command: list[str]
    elapsed_seconds: float
    return_code: int | None


@dataclass
class BenchmarkRun:
    source_path: Path
    timestamp: str
    profile: str
    rows: int
    consumer_messages: int
    loader_messages: int
    dry_run: bool
    total_elapsed_seconds: float
    steps: list[StepResult]


@dataclass
class ProfileAverage:
    profile: str
    run_count: int
    dry_run_count: int
    average_total_seconds: float


@dataclass
class AnalysisSummary:
    input_path: Path
    filtered_profile: str | None
    run_count: int
    dry_run_count: int
    real_run_count: int
    profiles_included: list[str]
    rows_message_groups: list[tuple[int, int, int, int]]
    step_totals: list[tuple[str, float, float]]
    runs: list[BenchmarkRun]
    fastest_run: BenchmarkRun | None
    slowest_run: BenchmarkRun | None
    profile_averages: list[ProfileAverage]
    bottleneck_lines: list[str]
    skipped_messages: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze local benchmark JSON files and summarize bottlenecks."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help="Benchmark JSON file or directory. Default: docs/performance/results.",
    )
    parser.add_argument(
        "--profile",
        choices=ALLOWED_PROFILES,
        default=None,
        help="Optional dataset profile filter.",
    )
    parser.add_argument(
        "--output-md",
        default=None,
        help="Optional Markdown report path or directory. Default: disabled.",
    )
    parser.add_argument(
        "--fail-on-missing-results",
        action="store_true",
        help="Return a non-zero exit code when no valid results are available.",
    )
    return parser.parse_args()


def resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = (REPO_ROOT / path_value).resolve()
    return path


def resolve_output_markdown_path(
    output_value: str | None, profile: str | None
) -> Path | None:
    if not output_value:
        return None

    output_path = resolve_path(output_value)
    if output_path.suffix.lower() == ".md":
        return output_path

    file_name = "performance-analysis.md"
    if profile:
        file_name = f"performance-analysis-{profile}.md"
    return output_path / file_name


def discover_input_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return sorted(path for path in input_path.iterdir() if path.suffix.lower() == ".json")
    return []


def require_type(value: Any, expected_type: type, field_name: str) -> Any:
    if not isinstance(value, expected_type):
        raise ValueError(f"{field_name} must be a {expected_type.__name__}.")
    return value


def require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer.")
    return value


def require_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric.")
    return float(value)


def require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean.")
    return value


def parse_step(step_payload: Any) -> StepResult:
    step_data = require_type(step_payload, dict, "command entry")
    name = require_type(step_data.get("name"), str, "command.name")
    command = require_type(step_data.get("command"), list, "command.command")
    if not all(isinstance(part, str) for part in command):
        raise ValueError("command.command must contain only strings.")

    elapsed_seconds = require_float(
        step_data.get("elapsed_seconds"), "command.elapsed_seconds"
    )
    return_code_value = step_data.get("return_code")
    if return_code_value is not None:
        return_code_value = require_int(return_code_value, "command.return_code")

    return StepResult(
        name=name,
        command=command,
        elapsed_seconds=elapsed_seconds,
        return_code=return_code_value,
    )


def load_benchmark_run(file_path: Path) -> BenchmarkRun:
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    data = require_type(payload, dict, "root JSON object")

    timestamp = require_type(data.get("timestamp"), str, "timestamp")
    profile = require_type(data.get("profile"), str, "profile")
    if profile not in ALLOWED_PROFILES:
        raise ValueError(f"profile must be one of: {', '.join(ALLOWED_PROFILES)}.")

    rows = require_int(data.get("rows"), "rows")
    consumer_messages = require_int(data.get("consumer_messages"), "consumer_messages")
    loader_messages = require_int(data.get("loader_messages"), "loader_messages")
    dry_run = require_bool(data.get("dry_run"), "dry_run")
    total_elapsed_seconds = require_float(
        data.get("total_elapsed_seconds"), "total_elapsed_seconds"
    )

    commands = require_type(data.get("commands"), list, "commands")
    steps = [parse_step(command_payload) for command_payload in commands]
    if not steps:
        raise ValueError("commands must contain at least one step.")

    return BenchmarkRun(
        source_path=file_path,
        timestamp=timestamp,
        profile=profile,
        rows=rows,
        consumer_messages=consumer_messages,
        loader_messages=loader_messages,
        dry_run=dry_run,
        total_elapsed_seconds=total_elapsed_seconds,
        steps=steps,
    )


def load_runs(
    input_path: Path, profile_filter: str | None
) -> tuple[list[BenchmarkRun], list[str]]:
    candidate_files = discover_input_files(input_path)
    skipped_messages: list[str] = []
    runs: list[BenchmarkRun] = []

    if not candidate_files:
        skipped_messages.append(f"No JSON files found under: {input_path}")
        return runs, skipped_messages

    for file_path in candidate_files:
        try:
            run = load_benchmark_run(file_path)
        except Exception as exc:
            skipped_messages.append(f"Skipped malformed file {file_path.name}: {exc}")
            continue

        if profile_filter and run.profile != profile_filter:
            continue
        runs.append(run)

    runs.sort(key=lambda run: (run.timestamp, run.source_path.name))
    return runs, skipped_messages


def compute_rows_message_groups(
    runs: list[BenchmarkRun],
) -> list[tuple[int, int, int, int]]:
    counts: dict[tuple[int, int, int], int] = {}
    for run in runs:
        key = (run.rows, run.consumer_messages, run.loader_messages)
        counts[key] = counts.get(key, 0) + 1

    return sorted(
        ((rows, consumer_messages, loader_messages, count) for (rows, consumer_messages, loader_messages), count in counts.items()),
        key=lambda item: (item[0], item[1], item[2]),
    )


def compute_step_totals(runs: list[BenchmarkRun]) -> list[tuple[str, float, float]]:
    step_names: list[str] = list(DEFAULT_STEP_ORDER)
    for run in runs:
        for step in run.steps:
            if step.name not in step_names:
                step_names.append(step.name)

    totals = {name: 0.0 for name in step_names}
    for run in runs:
        for step in run.steps:
            totals[step.name] = totals.get(step.name, 0.0) + step.elapsed_seconds

    total_elapsed = sum(totals.values())
    results: list[tuple[str, float, float]] = []
    for step_name in step_names:
        elapsed_seconds = totals.get(step_name, 0.0)
        share_percent = 0.0 if total_elapsed <= 0 else (elapsed_seconds / total_elapsed) * 100
        results.append((step_name, elapsed_seconds, share_percent))
    return results


def get_slowest_step_name(run: BenchmarkRun) -> str:
    if run.dry_run:
        return "not meaningful (dry-run)"

    slowest_step = run.steps[0]
    for step in run.steps[1:]:
        if step.elapsed_seconds > slowest_step.elapsed_seconds:
            slowest_step = step
    return slowest_step.name


def format_throughput(run: BenchmarkRun) -> str:
    if run.dry_run or run.total_elapsed_seconds <= 0:
        return "n/a"
    return f"{run.rows / run.total_elapsed_seconds:.2f}"


def compute_profile_averages(runs: list[BenchmarkRun]) -> list[ProfileAverage]:
    results: list[ProfileAverage] = []
    for profile in ALLOWED_PROFILES:
        profile_runs = [run for run in runs if run.profile == profile]
        if not profile_runs:
            continue

        results.append(
            ProfileAverage(
                profile=profile,
                run_count=len(profile_runs),
                dry_run_count=sum(1 for run in profile_runs if run.dry_run),
                average_total_seconds=statistics.mean(
                    run.total_elapsed_seconds for run in profile_runs
                ),
            )
        )
    return results


def build_bottleneck_interpretation(runs: list[BenchmarkRun]) -> list[str]:
    if not runs:
        return ["No valid benchmark results were available to analyze."]

    lines: list[str] = []
    dry_run_count = sum(1 for run in runs if run.dry_run)
    real_runs = [run for run in runs if not run.dry_run]

    if dry_run_count:
        lines.append(
            "Dry-run benchmark results were detected, so their timings are not meaningful for real performance analysis."
        )

    if not real_runs:
        lines.append(
            "Use at least one non-dry-run benchmark if you want real bottleneck analysis, throughput estimates, and timing comparisons."
        )
        return lines

    step_totals = compute_step_totals(real_runs)
    dominant_step = max(step_totals, key=lambda item: item[1])[0]
    slowest_step_counts: dict[str, int] = {}
    for run in real_runs:
        slowest_name = get_slowest_step_name(run)
        slowest_step_counts[slowest_name] = slowest_step_counts.get(slowest_name, 0) + 1

    slowest_frequency_name, slowest_frequency_count = max(
        slowest_step_counts.items(), key=lambda item: item[1]
    )
    lines.append(
        f"The dominant bottleneck across real runs was `{dominant_step}`, and the most frequent slowest step per run was `{slowest_frequency_name}` ({slowest_frequency_count}/{len(real_runs)} runs)."
    )

    if dominant_step == "go-producer":
        lines.append(
            "Focus on Kafka publish rate, producer send delay, batching strategy, and producer-side configuration if you want to reduce producer pressure."
        )
    elif dominant_step == "python-consumer":
        lines.append(
            "Focus on validation cost, payload deserialization, Kafka polling behavior, and consumer batch sizing if the Python consumer remains the slowest stage."
        )
    elif dominant_step == "warehouse-loader":
        lines.append(
            "Focus on database insert strategy, batching, indexes, and transaction handling if the warehouse loader remains the slowest stage."
        )
    else:
        lines.append(
            "Review the slowest step details in the per-run table before drawing optimization conclusions."
        )

    return lines


def build_analysis_summary(
    input_path: Path,
    filtered_profile: str | None,
    runs: list[BenchmarkRun],
    skipped_messages: list[str],
) -> AnalysisSummary:
    profiles_included = sorted({run.profile for run in runs})
    fastest_run = min(runs, key=lambda run: run.total_elapsed_seconds) if runs else None
    slowest_run = max(runs, key=lambda run: run.total_elapsed_seconds) if runs else None

    return AnalysisSummary(
        input_path=input_path,
        filtered_profile=filtered_profile,
        run_count=len(runs),
        dry_run_count=sum(1 for run in runs if run.dry_run),
        real_run_count=sum(1 for run in runs if not run.dry_run),
        profiles_included=profiles_included,
        rows_message_groups=compute_rows_message_groups(runs),
        step_totals=compute_step_totals(runs),
        runs=runs,
        fastest_run=fastest_run,
        slowest_run=slowest_run,
        profile_averages=compute_profile_averages(runs),
        bottleneck_lines=build_bottleneck_interpretation(runs),
        skipped_messages=skipped_messages,
    )


def print_analysis(summary: AnalysisSummary) -> None:
    print("Performance benchmark analysis")
    print(f"Input path: {summary.input_path}")
    if summary.filtered_profile:
        print(f"Profile filter: {summary.filtered_profile}")
    print(f"Valid benchmark runs: {summary.run_count}")
    print(f"Dry-run results: {summary.dry_run_count}")
    print(f"Real benchmark runs: {summary.real_run_count}")
    print(
        "Profiles included: "
        + (", ".join(summary.profiles_included) if summary.profiles_included else "none")
    )

    if summary.rows_message_groups:
        print("Rows/messages observed:")
        for rows, consumer_messages, loader_messages, count in summary.rows_message_groups:
            print(
                f"- rows={rows}, consumer_messages={consumer_messages}, "
                f"loader_messages={loader_messages} ({count} run(s))"
            )

    if summary.step_totals:
        print("Aggregate step totals:")
        for step_name, elapsed_seconds, share_percent in summary.step_totals:
            print(
                f"- {step_name}: {elapsed_seconds:.3f} sec total, "
                f"{share_percent:.2f}% share"
            )

    if summary.runs:
        print("Per-run overview:")
        for run in summary.runs:
            print(
                f"- {run.source_path.name}: profile={run.profile}, "
                f"total={run.total_elapsed_seconds:.3f} sec, "
                f"throughput_rows_per_sec={format_throughput(run)}, "
                f"slowest_step={get_slowest_step_name(run)}"
            )

    if summary.fastest_run is not None and summary.slowest_run is not None:
        print(
            "Fastest run: "
            f"{summary.fastest_run.source_path.name} "
            f"({summary.fastest_run.total_elapsed_seconds:.3f} sec)"
        )
        print(
            "Slowest run: "
            f"{summary.slowest_run.source_path.name} "
            f"({summary.slowest_run.total_elapsed_seconds:.3f} sec)"
        )

    if summary.profile_averages:
        print("Average total time by profile:")
        for profile_average in summary.profile_averages:
            print(
                f"- {profile_average.profile}: "
                f"{profile_average.average_total_seconds:.3f} sec average over "
                f"{profile_average.run_count} run(s)"
            )

    print("Bottleneck interpretation:")
    for line in summary.bottleneck_lines:
        print(f"- {line}")

    if summary.skipped_messages:
        print("Skipped files / notes:")
        for message in summary.skipped_messages:
            print(f"- {message}")


def make_markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    table_lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        table_lines.append("| " + " | ".join(row) + " |")
    return table_lines


def build_markdown_report(summary: AnalysisSummary) -> str:
    lines: list[str] = [
        "# Performance Analysis",
        "",
        f"- Input path: `{summary.input_path}`",
        f"- Profile filter: `{summary.filtered_profile or 'all'}`",
        f"- Valid benchmark runs: `{summary.run_count}`",
        f"- Dry-run results: `{summary.dry_run_count}`",
        f"- Real benchmark runs: `{summary.real_run_count}`",
        "- Profiles included: "
        + (
            ", ".join(f"`{profile}`" for profile in summary.profiles_included)
            if summary.profiles_included
            else "`none`"
        ),
        "",
        "## Rows and Message Caps",
        "",
    ]

    if summary.rows_message_groups:
        for rows, consumer_messages, loader_messages, count in summary.rows_message_groups:
            lines.append(
                f"- rows=`{rows}`, consumer_messages=`{consumer_messages}`, "
                f"loader_messages=`{loader_messages}` across `{count}` run(s)"
            )
    else:
        lines.append("- No valid runs were available.")

    lines.extend(["", "## Aggregate Step Breakdown", ""])
    aggregate_rows = [
        [step_name, f"{elapsed_seconds:.3f}", f"{share_percent:.2f}%"]
        for step_name, elapsed_seconds, share_percent in summary.step_totals
    ]
    if aggregate_rows:
        lines.extend(
            make_markdown_table(
                ["Step", "Elapsed seconds", "Percent share"], aggregate_rows
            )
        )
    else:
        lines.append("No aggregate step timings were available.")

    lines.extend(["", "## Per-Run Summary", ""])
    per_run_rows = [
        [
            run.source_path.name,
            run.timestamp,
            run.profile,
            str(run.rows),
            str(run.consumer_messages),
            str(run.loader_messages),
            f"{run.total_elapsed_seconds:.3f}",
            format_throughput(run),
            get_slowest_step_name(run),
        ]
        for run in summary.runs
    ]
    if per_run_rows:
        lines.extend(
            make_markdown_table(
                [
                    "File",
                    "Timestamp",
                    "Profile",
                    "Rows",
                    "Consumer messages",
                    "Loader messages",
                    "Total seconds",
                    "Throughput rows/sec",
                    "Slowest step",
                ],
                per_run_rows,
            )
        )
    else:
        lines.append("No valid benchmark runs were available.")

    lines.extend(["", "## Fastest and Slowest Run", ""])
    if summary.fastest_run is not None and summary.slowest_run is not None:
        lines.append(
            f"- Fastest run: `{summary.fastest_run.source_path.name}` at `{summary.fastest_run.total_elapsed_seconds:.3f}` seconds"
        )
        lines.append(
            f"- Slowest run: `{summary.slowest_run.source_path.name}` at `{summary.slowest_run.total_elapsed_seconds:.3f}` seconds"
        )
    else:
        lines.append("- No valid benchmark runs were available.")

    lines.extend(["", "## Average Total Time by Profile", ""])
    average_rows = [
        [
            profile_average.profile,
            str(profile_average.run_count),
            str(profile_average.dry_run_count),
            f"{profile_average.average_total_seconds:.3f}",
        ]
        for profile_average in summary.profile_averages
    ]
    if average_rows:
        lines.extend(
            make_markdown_table(
                ["Profile", "Run count", "Dry-run count", "Average total seconds"],
                average_rows,
            )
        )
    else:
        lines.append("No profile averages were available.")

    lines.extend(["", "## Bottleneck Interpretation", ""])
    for line in summary.bottleneck_lines:
        lines.append(f"- {line}")

    if summary.skipped_messages:
        lines.extend(["", "## Skipped Files / Notes", ""])
        for message in summary.skipped_messages:
            lines.append(f"- {message}")

    lines.extend(
        [
            "",
            "## Note",
            "",
            "- Dry-run benchmark files are useful for checking metadata flow, but their timings should not be treated as real performance measurements.",
            "- Real benchmark conclusions still depend on the local machine, Docker resource allocation, and other concurrent workloads.",
            "",
        ]
    )
    return "\n".join(lines)


def write_markdown_report(output_path: Path, summary: AnalysisSummary) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(build_markdown_report(summary), encoding="utf-8")


def main() -> int:
    args = parse_args()
    input_path = resolve_path(args.input)
    output_md_path = resolve_output_markdown_path(args.output_md, args.profile)

    runs, skipped_messages = load_runs(input_path, args.profile)
    summary = build_analysis_summary(input_path, args.profile, runs, skipped_messages)
    print_analysis(summary)

    if output_md_path is not None and summary.run_count > 0:
        write_markdown_report(output_md_path, summary)
        print(f"Markdown analysis written to: {output_md_path}")

    if summary.run_count == 0:
        message = "No valid benchmark results were found."
        if args.fail_on_missing_results:
            print(message, file=sys.stderr)
            return 1
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
