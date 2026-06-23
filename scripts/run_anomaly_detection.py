#!/usr/bin/env python3
"""Run local rule-based anomaly detection against recent processed IoT logs.

Stage 18A intentionally avoids external Python dependencies. The standard
library does not provide a PostgreSQL client, so this script shells out to
`docker compose exec -T postgres psql ...` and parses CSV output from `psql`.
That keeps the job dependency-free while still working with the repository's
local Docker Compose PostgreSQL service. Stage 18B extends the same approach
to optional warehouse persistence into `iot_anomalies`.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANOMALIES_TABLE_SQL_PATH = PROJECT_ROOT / "storage" / "postgres" / "init" / "04_create_iot_anomalies.sql"
ANOMALIES_TABLE_CONTAINER_SQL_PATH = "/docker-entrypoint-initdb.d/04_create_iot_anomalies.sql"
DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = "5432"
DEFAULT_POSTGRES_DB = "iot_logs"
DEFAULT_POSTGRES_USER = "iot_user"
DEFAULT_POSTGRES_PASSWORD = "iot_password"

EXPECTED_PROTOCOLS = {"TCP", "UDP", "ICMP"}
UNUSUAL_BUT_ALLOWED_PROTOCOLS = {"ICMP"}
BENIGN_ATTACK_TYPES = {"", "benign", "none", "normal", "unknown", "n/a", "na"}
HIGH_RISK_ATTACK_TYPES = {
    "arp_poisoning",
    "arp_poisioning",
    "botnet",
    "brute_force",
    "ddos",
    "dos",
    "malware",
    "mitm",
    "port_scan",
    "ransomware",
    "sql_injection",
}


@dataclass(frozen=True)
class ProcessedLogRow:
    source_row_id: int | None
    device_id: str
    event_timestamp: str
    protocol: str
    packet_size: int | None
    duration_ms: int | None
    attack_type: str
    status: str


@dataclass(frozen=True)
class AnomalyRecord:
    source_row_id: int | None
    device_id: str
    event_timestamp: str
    rule_name: str
    severity: str
    reason: str
    score: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local rule-based anomaly detection on processed_iot_logs."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of recent rows to scan. Default: 1000.",
    )
    parser.add_argument(
        "--output-json",
        help="Optional path for a JSON anomaly report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview anomalies without any database writes.",
    )
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Insert detected anomalies into the iot_anomalies warehouse table.",
    )
    parser.add_argument(
        "--run-id",
        help="Optional run id. Defaults to a generated UTC timestamp-based id.",
    )
    parser.add_argument(
        "--ensure-table",
        action="store_true",
        help="Ensure the iot_anomalies table exists before continuing.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print additional execution details and anomaly samples.",
    )
    return parser.parse_args()


def get_connection_settings() -> dict[str, str]:
    return {
        "host": os.getenv("POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
        "port": os.getenv("POSTGRES_PORT", DEFAULT_POSTGRES_PORT),
        "db": os.getenv("POSTGRES_DB", DEFAULT_POSTGRES_DB),
        "user": os.getenv("POSTGRES_USER", DEFAULT_POSTGRES_USER),
        "password": os.getenv("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
    }


def normalize_host(host: str) -> str:
    return "127.0.0.1" if host.strip().lower() == "localhost" else host.strip()


def generate_run_id() -> str:
    return "anomaly-detection-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_copy_query(limit: int) -> str:
    return f"""
COPY (
    SELECT
        id AS source_row_id,
        COALESCE(
            to_char(
                event_timestamp AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
            ),
            ''
        ) AS event_timestamp,
        COALESCE(device_id, '') AS device_id,
        COALESCE(protocol, '') AS protocol,
        COALESCE(packet_size::text, '') AS packet_size,
        COALESCE(duration_ms::text, '') AS duration_ms,
        COALESCE(attack_type, '') AS attack_type,
        COALESCE(status, '') AS status
    FROM processed_iot_logs
    ORDER BY COALESCE(processed_at, inserted_at, event_timestamp) DESC NULLS LAST, id DESC
    LIMIT {limit}
) TO STDOUT WITH CSV HEADER
""".strip()


def build_psql_command(settings: dict[str, str]) -> list[str]:
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        f"PGPASSWORD={settings['password']}",
        "postgres",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "pager=off",
        "-h",
        normalize_host(settings["host"]),
        "-p",
        settings["port"],
        "-U",
        settings["user"],
        "-d",
        settings["db"],
    ]

 
def redact_command(command: list[str]) -> str:
    return " ".join(
        part for part in command if not part.startswith("PGPASSWORD=")
    )


def run_psql(
    settings: dict[str, str],
    extra_args: list[str],
    *,
    input_text: str | None = None,
    verbose: bool,
    operation_name: str,
) -> subprocess.CompletedProcess[str]:
    command = build_psql_command(settings) + extra_args
    if verbose:
        print(f"Running PostgreSQL {operation_name} via docker compose exec:")
        print(redact_command(command))

    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Docker CLI was not found. Install Docker Desktop and ensure `docker` is on PATH."
        ) from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        guidance = ""
        if "is not running" in stderr.lower():
            guidance = " Start PostgreSQL first with `docker compose up -d postgres`."
        raise RuntimeError(
            f"Failed to complete PostgreSQL {operation_name} via Docker Compose and psql. "
            f"{stderr}{guidance}"
        )
    return result


def fetch_recent_rows(limit: int, settings: dict[str, str], verbose: bool) -> list[ProcessedLogRow]:
    result = run_psql(
        settings=settings,
        extra_args=["-c", build_copy_query(limit)],
        verbose=verbose,
        operation_name="read query",
    )
    reader = csv.DictReader(io.StringIO(result.stdout))
    rows: list[ProcessedLogRow] = []
    for raw_row in reader:
        rows.append(
            ProcessedLogRow(
                source_row_id=parse_optional_int(raw_row.get("source_row_id", "")),
                device_id=(raw_row.get("device_id") or "").strip(),
                event_timestamp=(raw_row.get("event_timestamp") or "").strip(),
                protocol=(raw_row.get("protocol") or "").strip().upper(),
                packet_size=parse_optional_int(raw_row.get("packet_size", "")),
                duration_ms=parse_optional_int(raw_row.get("duration_ms", "")),
                attack_type=(raw_row.get("attack_type") or "").strip(),
                status=(raw_row.get("status") or "").strip().lower(),
            )
        )
    return rows


def parse_optional_int(value: str) -> int | None:
    normalized = value.strip()
    if not normalized:
        return None
    return int(normalized)


def detect_anomalies(rows: list[ProcessedLogRow]) -> list[AnomalyRecord]:
    anomalies: list[AnomalyRecord] = []
    for row in rows:
        anomalies.extend(detect_row_anomalies(row))
    return anomalies


def detect_row_anomalies(row: ProcessedLogRow) -> list[AnomalyRecord]:
    anomalies: list[AnomalyRecord] = []

    if row.packet_size is not None:
        if row.packet_size >= 1500:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="high_packet_size",
                    severity="high",
                    score=90,
                    reason=f"packet_size={row.packet_size} exceeds the high threshold of 1500 bytes.",
                )
            )
        elif row.packet_size >= 1000:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="high_packet_size",
                    severity="medium",
                    score=70,
                    reason=f"packet_size={row.packet_size} exceeds the review threshold of 1000 bytes.",
                )
            )

    if row.duration_ms is not None:
        if row.duration_ms >= 15000:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="high_duration",
                    severity="high",
                    score=85,
                    reason=f"duration_ms={row.duration_ms} exceeds the high threshold of 15000 ms.",
                )
            )
        elif row.duration_ms >= 5000:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="high_duration",
                    severity="medium",
                    score=65,
                    reason=f"duration_ms={row.duration_ms} exceeds the review threshold of 5000 ms.",
                )
            )

    if row.status in {"blocked", "denied", "dropped"}:
        anomalies.append(
            build_anomaly(
                row,
                rule_name="failed_or_blocked_status",
                severity="high",
                score=95,
                reason=f"status={row.status} indicates traffic that was blocked or denied.",
            )
        )
    elif row.status in {"failed", "error"}:
        anomalies.append(
            build_anomaly(
                row,
                rule_name="failed_or_blocked_status",
                severity="medium",
                score=75,
                reason=f"status={row.status} indicates unsuccessful processing or failed traffic.",
            )
        )

    normalized_attack_type = row.attack_type.strip().lower()
    if normalized_attack_type and normalized_attack_type not in BENIGN_ATTACK_TYPES:
        if normalized_attack_type in HIGH_RISK_ATTACK_TYPES:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="suspicious_attack_type",
                    severity="high",
                    score=92,
                    reason=f"attack_type={row.attack_type} matches a high-risk suspicious traffic label.",
                )
            )
        else:
            anomalies.append(
                build_anomaly(
                    row,
                    rule_name="suspicious_attack_type",
                    severity="medium",
                    score=68,
                    reason=f"attack_type={row.attack_type} is non-empty and marked for analyst review.",
                )
            )

    if row.protocol and row.protocol not in EXPECTED_PROTOCOLS:
        anomalies.append(
            build_anomaly(
                row,
                rule_name="unusual_protocol",
                severity="high",
                score=80,
                reason=f"protocol={row.protocol} is outside the expected set {sorted(EXPECTED_PROTOCOLS)}.",
            )
        )
    elif row.protocol in UNUSUAL_BUT_ALLOWED_PROTOCOLS:
        anomalies.append(
            build_anomaly(
                row,
                rule_name="unusual_protocol",
                severity="low",
                score=35,
                reason=f"protocol={row.protocol} is allowed but treated as less common control-plane traffic.",
            )
        )

    return anomalies


def build_anomaly(
    row: ProcessedLogRow,
    rule_name: str,
    severity: str,
    score: int,
    reason: str,
) -> AnomalyRecord:
    return AnomalyRecord(
        source_row_id=row.source_row_id,
        device_id=row.device_id,
        event_timestamp=row.event_timestamp,
        rule_name=rule_name,
        severity=severity,
        reason=reason,
        score=score,
    )


def ensure_anomalies_table(settings: dict[str, str], verbose: bool) -> None:
    if not ANOMALIES_TABLE_SQL_PATH.is_file():
        raise FileNotFoundError(
            f"Anomaly table SQL file not found: {ANOMALIES_TABLE_SQL_PATH}"
        )
    run_psql(
        settings=settings,
        extra_args=["-v", "ON_ERROR_STOP=1", "-f", ANOMALIES_TABLE_CONTAINER_SQL_PATH],
        verbose=verbose,
        operation_name="table ensure",
    )


def serialize_anomalies_for_copy(run_id: str, anomalies: list[AnomalyRecord]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "run_id",
            "source_row_id",
            "event_timestamp",
            "device_id",
            "rule_name",
            "severity",
            "reason",
            "score",
        ]
    )
    for anomaly in anomalies:
        writer.writerow(
            [
                run_id,
                "" if anomaly.source_row_id is None else str(anomaly.source_row_id),
                anomaly.event_timestamp,
                anomaly.device_id,
                anomaly.rule_name,
                anomaly.severity,
                anomaly.reason,
                anomaly.score,
            ]
        )
    return buffer.getvalue()


def write_anomalies_to_db(
    settings: dict[str, str],
    run_id: str,
    anomalies: list[AnomalyRecord],
    verbose: bool,
) -> int:
    if not anomalies:
        return 0

    copy_payload = serialize_anomalies_for_copy(run_id=run_id, anomalies=anomalies)
    copy_sql = """
COPY iot_anomalies (
    run_id,
    source_row_id,
    event_timestamp,
    device_id,
    rule_name,
    severity,
    reason,
    score
) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')
""".strip()
    run_psql(
        settings=settings,
        extra_args=["-c", copy_sql],
        input_text=copy_payload,
        verbose=verbose,
        operation_name="anomaly insert",
    )
    return len(anomalies)


def build_summary(
    *,
    run_id: str,
    rows: list[ProcessedLogRow],
    anomalies: list[AnomalyRecord],
    dry_run: bool,
    write_db: bool,
    inserted_rows: int,
) -> dict[str, Any]:
    rule_counts = Counter(anomaly.rule_name for anomaly in anomalies)
    severity_counts = Counter(anomaly.severity for anomaly in anomalies)
    affected_rows = {anomaly.source_row_id for anomaly in anomalies if anomaly.source_row_id is not None}

    return {
        "run_id": run_id,
        "dry_run": dry_run,
        "write_db": write_db,
        "rows_scanned": len(rows),
        "anomalies_detected": len(anomalies),
        "rows_with_anomalies": len(affected_rows),
        "inserted_rows": inserted_rows,
        "anomalies_by_rule": dict(sorted(rule_counts.items())),
        "anomalies_by_severity": dict(sorted(severity_counts.items())),
    }


def print_summary(summary: dict[str, Any], anomalies: list[AnomalyRecord], verbose: bool) -> None:
    print("Anomaly detection summary")
    print(f"Run id: {summary['run_id']}")
    print(f"Dry run: {'yes' if summary['dry_run'] else 'no'}")
    print(f"Write to DB: {'yes' if summary['write_db'] else 'no'}")
    print(f"Rows scanned: {summary['rows_scanned']}")
    print(f"Anomalies detected: {summary['anomalies_detected']}")
    print(f"Rows with anomalies: {summary['rows_with_anomalies']}")
    print(f"Inserted rows: {summary['inserted_rows']}")
    print("Anomalies by rule:")
    if summary["anomalies_by_rule"]:
        for rule_name, count in summary["anomalies_by_rule"].items():
            print(f"  {rule_name}: {count}")
    else:
        print("  none")
    print("Anomalies by severity:")
    if summary["anomalies_by_severity"]:
        for severity, count in summary["anomalies_by_severity"].items():
            print(f"  {severity}: {count}")
    else:
        print("  none")

    if verbose and anomalies:
        print("Anomaly samples:")
        for anomaly in anomalies[:20]:
            row_id = anomaly.source_row_id if anomaly.source_row_id is not None else "n/a"
            print(
                f"  row_id={row_id} device_id={anomaly.device_id or 'n/a'} "
                f"rule={anomaly.rule_name} severity={anomaly.severity} score={anomaly.score}"
            )
            print(f"    reason={anomaly.reason}")


def write_json_report(
    output_path: Path,
    settings: dict[str, str],
    limit: int,
    run_id: str,
    dry_run: bool,
    write_db: bool,
    summary: dict[str, Any],
    anomalies: list[AnomalyRecord],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_id": run_id,
        "mode": "dry-run" if dry_run else "write-db" if write_db else "read-only",
        "limit": limit,
        "connection": {
            "host": settings["host"],
            "port": settings["port"],
            "database": settings["db"],
            "user": settings["user"],
        },
        "summary": summary,
        "anomalies": [asdict(anomaly) for anomaly in anomalies],
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_path = Path(args.output_json) if args.output_json else None

    if args.limit <= 0:
        print("Error: --limit must be a positive integer.", file=sys.stderr)
        return 1
    if args.dry_run and args.write_db:
        print("Error: --dry-run cannot be combined with --write-db.", file=sys.stderr)
        return 1
    if args.dry_run and args.ensure_table:
        print("Error: --dry-run cannot be combined with --ensure-table.", file=sys.stderr)
        return 1

    try:
        settings = get_connection_settings()
        run_id = args.run_id or generate_run_id()
        if args.ensure_table:
            ensure_anomalies_table(settings=settings, verbose=args.verbose)
        rows = fetch_recent_rows(limit=args.limit, settings=settings, verbose=args.verbose)
        anomalies = detect_anomalies(rows)
        inserted_rows = 0
        if args.write_db:
            inserted_rows = write_anomalies_to_db(
                settings=settings,
                run_id=run_id,
                anomalies=anomalies,
                verbose=args.verbose,
            )
        summary = build_summary(
            run_id=run_id,
            rows=rows,
            anomalies=anomalies,
            dry_run=args.dry_run,
            write_db=args.write_db,
            inserted_rows=inserted_rows,
        )
        print_summary(summary=summary, anomalies=anomalies, verbose=args.verbose)

        if args.dry_run:
            print("Dry-run mode confirms that no database writes are attempted.")
        elif args.write_db:
            print(f"Inserted anomaly rows into iot_anomalies: {inserted_rows}")
        else:
            print("Read-only mode confirms that no database writes were attempted.")

        if args.ensure_table:
            print("Ensured iot_anomalies table exists.")

        if output_path is not None:
            write_json_report(
                output_path=output_path,
                settings=settings,
                limit=args.limit,
                run_id=run_id,
                dry_run=args.dry_run,
                write_db=args.write_db,
                summary=summary,
                anomalies=anomalies,
            )
            print(f"JSON report written to: {output_path}")
        return 0
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
