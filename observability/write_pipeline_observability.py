from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg


DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DB = "iot_logs"
DEFAULT_POSTGRES_USER = "iot_user"
DEFAULT_POSTGRES_PASSWORD = "iot_password"
DEFAULT_PIPELINE_NAME = "iot_local_pipeline"
DEFAULT_ENVIRONMENT = "local"
DEFAULT_INVALID_RATE_THRESHOLD = Decimal("0.20")
DEFAULT_MIN_PROCESSED_RECORDS = 1
DECIMAL_SCALE = Decimal("0.000001")


REQUIRED_TABLE_COLUMNS: dict[str, set[str]] = {
    "processed_iot_logs": {"id"},
    "invalid_iot_logs": {"id"},
    "pipeline_run_audit": {
        "id",
        "run_id",
        "pipeline_name",
        "environment",
        "started_at",
        "finished_at",
        "status",
        "processed_records",
        "invalid_records",
        "invalid_rate",
        "high_risk_devices",
        "total_alerts",
        "created_at",
    },
    "pipeline_quality_checks": {
        "id",
        "run_id",
        "check_name",
        "check_status",
        "severity",
        "metric_name",
        "metric_value",
        "threshold_value",
        "message",
        "created_at",
    },
    "pipeline_alerts": {
        "id",
        "run_id",
        "alert_type",
        "alert_level",
        "alert_message",
        "source",
        "is_published_to_kafka",
        "created_at",
    },
}


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class WriterConfig:
    run_id: str
    pipeline_name: str
    environment: str
    invalid_rate_threshold: Decimal
    min_processed_records: int


@dataclass(frozen=True)
class PipelineMetrics:
    processed_records: int
    invalid_records: int
    invalid_rate: Decimal
    high_risk_devices: int


@dataclass(frozen=True)
class QualityCheck:
    check_name: str
    check_status: str
    severity: str
    metric_name: str
    metric_value: Decimal
    threshold_value: Decimal
    message: str


@dataclass(frozen=True)
class AlertRecord:
    alert_type: str
    alert_level: str
    alert_message: str
    source: str = "observability_writer"
    is_published_to_kafka: bool = False


def parse_args() -> WriterConfig:
    parser = argparse.ArgumentParser(
        description="Write pipeline observability metrics into PostgreSQL audit tables.",
    )
    parser.add_argument("--run-id", required=True, help="Unique pipeline run identifier.")
    parser.add_argument(
        "--pipeline-name",
        default=DEFAULT_PIPELINE_NAME,
        help=f"Pipeline name stored in observability tables. Default: {DEFAULT_PIPELINE_NAME}",
    )
    parser.add_argument(
        "--environment",
        default=DEFAULT_ENVIRONMENT,
        help=f"Environment label stored in observability tables. Default: {DEFAULT_ENVIRONMENT}",
    )
    parser.add_argument(
        "--invalid-rate-threshold",
        default=str(DEFAULT_INVALID_RATE_THRESHOLD),
        type=parse_decimal,
        help="Maximum allowed invalid rate. Default: 0.20",
    )
    parser.add_argument(
        "--min-processed-records",
        default=DEFAULT_MIN_PROCESSED_RECORDS,
        type=parse_non_negative_int,
        help="Minimum processed_iot_logs row count required for a passing check. Default: 1",
    )
    args = parser.parse_args()

    return WriterConfig(
        run_id=args.run_id.strip(),
        pipeline_name=args.pipeline_name.strip(),
        environment=args.environment.strip(),
        invalid_rate_threshold=args.invalid_rate_threshold,
        min_processed_records=args.min_processed_records,
    )


def load_database_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", DEFAULT_POSTGRES_HOST).strip() or DEFAULT_POSTGRES_HOST,
        port=parse_port(os.getenv("POSTGRES_PORT", str(DEFAULT_POSTGRES_PORT))),
        database=os.getenv("POSTGRES_DB", DEFAULT_POSTGRES_DB).strip() or DEFAULT_POSTGRES_DB,
        user=os.getenv("POSTGRES_USER", DEFAULT_POSTGRES_USER).strip() or DEFAULT_POSTGRES_USER,
        password=os.getenv("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD).strip()
        or DEFAULT_POSTGRES_PASSWORD,
    )


def main() -> None:
    config = parse_args()
    validate_writer_config(config)

    database_config = load_database_config()
    started_at = datetime.now(timezone.utc)

    with psycopg.connect(
        host=database_config.host,
        port=database_config.port,
        dbname=database_config.database,
        user=database_config.user,
        password=database_config.password,
        connect_timeout=10,
    ) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                validate_schema(cursor)
                metrics = fetch_metrics(cursor)
                quality_checks = build_quality_checks(metrics=metrics, config=config)
                alerts = build_alerts(quality_checks)
                finished_at = datetime.now(timezone.utc)
                status = "success" if not alerts else "warning"

                upsert_pipeline_run_audit(
                    cursor=cursor,
                    config=config,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=status,
                    metrics=metrics,
                    total_alerts=len(alerts),
                )
                replace_quality_checks(cursor=cursor, run_id=config.run_id, checks=quality_checks)
                replace_alerts(cursor=cursor, run_id=config.run_id, alerts=alerts)

    print(f"run_id={config.run_id}")
    print(f"pipeline_name={config.pipeline_name}")
    print(f"environment={config.environment}")
    print(f"processed_records={metrics.processed_records}")
    print(f"invalid_records={metrics.invalid_records}")
    print(f"invalid_rate={format_decimal(metrics.invalid_rate)}")
    print(f"quality_checks={len(quality_checks)}")
    print(f"alerts={len(alerts)}")


def validate_writer_config(config: WriterConfig) -> None:
    if not config.run_id:
        raise ValueError("--run-id cannot be empty")
    if not config.pipeline_name:
        raise ValueError("--pipeline-name cannot be empty")
    if not config.environment:
        raise ValueError("--environment cannot be empty")


def validate_schema(cursor: psycopg.Cursor) -> None:
    for table_name, required_columns in REQUIRED_TABLE_COLUMNS.items():
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table_name,),
        )
        existing_columns = {row[0] for row in cursor.fetchall()}
        if not existing_columns:
            raise RuntimeError(f"required table '{table_name}' does not exist")

        missing_columns = sorted(required_columns - existing_columns)
        if missing_columns:
            joined_columns = ", ".join(missing_columns)
            raise RuntimeError(f"table '{table_name}' is missing required columns: {joined_columns}")


def fetch_metrics(cursor: psycopg.Cursor) -> PipelineMetrics:
    cursor.execute("SELECT COUNT(*) FROM processed_iot_logs")
    processed_records = int(cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM invalid_iot_logs")
    invalid_records = int(cursor.fetchone()[0])

    total_records = processed_records + invalid_records
    invalid_rate = Decimal("0")
    if total_records > 0:
        invalid_rate = quantize_decimal(Decimal(invalid_records) / Decimal(total_records))

    return PipelineMetrics(
        processed_records=processed_records,
        invalid_records=invalid_records,
        invalid_rate=invalid_rate,
        high_risk_devices=0,
    )


def build_quality_checks(metrics: PipelineMetrics, config: WriterConfig) -> list[QualityCheck]:
    processed_status = "pass" if metrics.processed_records >= config.min_processed_records else "fail"
    processed_severity = "low" if processed_status == "pass" else "high"
    processed_message = (
        f"Processed record count {metrics.processed_records} meets the minimum threshold "
        f"of {config.min_processed_records}."
        if processed_status == "pass"
        else f"Processed record count {metrics.processed_records} is below the minimum threshold "
        f"of {config.min_processed_records}."
    )

    invalid_rate_status = (
        "pass" if metrics.invalid_rate <= config.invalid_rate_threshold else "fail"
    )
    invalid_rate_severity = "low" if invalid_rate_status == "pass" else "medium"
    invalid_rate_message = (
        f"Invalid rate {format_decimal(metrics.invalid_rate)} is within the threshold "
        f"of {format_decimal(config.invalid_rate_threshold)}."
        if invalid_rate_status == "pass"
        else f"Invalid rate {format_decimal(metrics.invalid_rate)} exceeds the threshold "
        f"of {format_decimal(config.invalid_rate_threshold)}."
    )

    return [
        QualityCheck(
            check_name="processed_records_present",
            check_status=processed_status,
            severity=processed_severity,
            metric_name="processed_records",
            metric_value=Decimal(metrics.processed_records),
            threshold_value=Decimal(config.min_processed_records),
            message=processed_message,
        ),
        QualityCheck(
            check_name="invalid_rate_threshold",
            check_status=invalid_rate_status,
            severity=invalid_rate_severity,
            metric_name="invalid_rate",
            metric_value=metrics.invalid_rate,
            threshold_value=config.invalid_rate_threshold,
            message=invalid_rate_message,
        ),
    ]


def build_alerts(quality_checks: list[QualityCheck]) -> list[AlertRecord]:
    alerts: list[AlertRecord] = []
    for check in quality_checks:
        if check.check_status == "pass":
            continue

        alert_level = "warning" if check.severity in {"low", "medium"} else "critical"
        alerts.append(
            AlertRecord(
                alert_type=check.check_name,
                alert_level=alert_level,
                alert_message=check.message,
            )
        )

    return alerts


def upsert_pipeline_run_audit(
    cursor: psycopg.Cursor,
    config: WriterConfig,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    metrics: PipelineMetrics,
    total_alerts: int,
) -> None:
    cursor.execute(
        """
        SELECT id
        FROM pipeline_run_audit
        WHERE run_id = %s
        ORDER BY id
        FOR UPDATE
        """,
        (config.run_id,),
    )
    existing_ids = [int(row[0]) for row in cursor.fetchall()]

    if existing_ids:
        primary_id = existing_ids[0]
        cursor.execute(
            """
            UPDATE pipeline_run_audit
            SET pipeline_name = %s,
                environment = %s,
                started_at = %s,
                finished_at = %s,
                status = %s,
                processed_records = %s,
                invalid_records = %s,
                invalid_rate = %s,
                high_risk_devices = %s,
                total_alerts = %s
            WHERE id = %s
            """,
            (
                config.pipeline_name,
                config.environment,
                started_at,
                finished_at,
                status,
                metrics.processed_records,
                metrics.invalid_records,
                metrics.invalid_rate,
                metrics.high_risk_devices,
                total_alerts,
                primary_id,
            ),
        )

        if len(existing_ids) > 1:
            cursor.execute(
                "DELETE FROM pipeline_run_audit WHERE run_id = %s AND id <> %s",
                (config.run_id, primary_id),
            )
        return

    cursor.execute(
        """
        INSERT INTO pipeline_run_audit (
            run_id,
            pipeline_name,
            environment,
            started_at,
            finished_at,
            status,
            processed_records,
            invalid_records,
            invalid_rate,
            high_risk_devices,
            total_alerts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            config.run_id,
            config.pipeline_name,
            config.environment,
            started_at,
            finished_at,
            status,
            metrics.processed_records,
            metrics.invalid_records,
            metrics.invalid_rate,
            metrics.high_risk_devices,
            total_alerts,
        ),
    )


def replace_quality_checks(
    cursor: psycopg.Cursor,
    run_id: str,
    checks: list[QualityCheck],
) -> None:
    cursor.execute("DELETE FROM pipeline_quality_checks WHERE run_id = %s", (run_id,))
    cursor.executemany(
        """
        INSERT INTO pipeline_quality_checks (
            run_id,
            check_name,
            check_status,
            severity,
            metric_name,
            metric_value,
            threshold_value,
            message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_id,
                check.check_name,
                check.check_status,
                check.severity,
                check.metric_name,
                check.metric_value,
                check.threshold_value,
                check.message,
            )
            for check in checks
        ],
    )


def replace_alerts(
    cursor: psycopg.Cursor,
    run_id: str,
    alerts: list[AlertRecord],
) -> None:
    cursor.execute("DELETE FROM pipeline_alerts WHERE run_id = %s", (run_id,))
    if not alerts:
        return

    cursor.executemany(
        """
        INSERT INTO pipeline_alerts (
            run_id,
            alert_type,
            alert_level,
            alert_message,
            source,
            is_published_to_kafka
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_id,
                alert.alert_type,
                alert.alert_level,
                alert.alert_message,
                alert.source,
                alert.is_published_to_kafka,
            )
            for alert in alerts
        ],
    )


def parse_decimal(raw_value: str) -> Decimal:
    try:
        value = Decimal(raw_value)
    except Exception as exc:  # noqa: BLE001
        raise argparse.ArgumentTypeError("value must be a valid decimal number") from exc

    if value < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")

    return quantize_decimal(value)


def parse_non_negative_int(raw_value: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc

    if value < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")

    return value


def parse_port(raw_value: str) -> int:
    port = parse_non_negative_int(raw_value)
    if port == 0:
        raise ValueError("POSTGRES_PORT must be greater than zero")
    return port


def quantize_decimal(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_SCALE, rounding=ROUND_HALF_UP)


def format_decimal(value: Decimal) -> str:
    return f"{quantize_decimal(value):f}"


if __name__ == "__main__":
    main()
