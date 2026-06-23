# Anomaly Detection / Suspicious Traffic Detection

Stage 18 introduces a local anomaly-detection layer for processed IoT logs. Stage 18A added the standalone rule-based job and local validation workflow. Stage 18B extends that foundation by persisting anomaly results into PostgreSQL warehouse table `iot_anomalies`. Stage 18C now integrates anomaly detection into the local Airflow pipeline after warehouse loading.

## What Stage 18 is for

The Stage 18 goal is to identify suspicious or outlier behavior in curated IoT traffic that has already passed the existing Kafka, consumer, warehouse, dbt, Spark, and observability foundations.

At this stage, anomaly detection is intentionally local-first and safe by default:

- it scans recent rows from `processed_iot_logs`
- it applies lightweight heuristics
- it prints a console summary
- it can write a local JSON report for inspection
- it can optionally persist anomaly rows into `iot_anomalies`

Stage 18 still does not:

- change the Go producer, Python consumer, warehouse loader, dbt, Spark, MinIO, Terraform, benchmark, or data contract runtime logic
- add cloud or production anomaly orchestration

## Why rule-based anomaly detection comes before ML

Rule-based detection is useful before ML because it gives a fast, explainable baseline:

- it is easy to validate locally against known fields such as `packet_size`, `duration_ms`, `status`, `attack_type`, and `protocol`
- each anomaly can include a human-readable reason and severity immediately
- thresholds and suspicious-value lists are easy to tune before investing in feature engineering or model training
- the output can later become labeled input, QA feedback, or benchmark data for future ML stages

This keeps Stage 18A, 18B, and 18C simple and transparent while the rest of the local data platform stays stable.

## Stage 18A/18B/18C rules

The Stage 18A job lives at [`scripts/run_anomaly_detection.py`](../scripts/run_anomaly_detection.py).

It currently applies these rule families:

- `high_packet_size`
  - medium severity when `packet_size >= 1000`
  - high severity when `packet_size >= 1500`
- `high_duration`
  - medium severity when `duration_ms >= 5000`
  - high severity when `duration_ms >= 15000`
- `failed_or_blocked_status`
  - medium severity for `failed` or `error`
  - high severity for `blocked`, `denied`, or `dropped`
- `suspicious_attack_type`
  - high severity for known suspicious labels such as `DDoS`, `ARP_poisoning`, `botnet`, `malware`, `MITM`, or `port_scan`
  - medium severity for other non-empty, non-benign attack labels that should be reviewed
- `unusual_protocol`
  - high severity if a protocol is outside the expected `TCP`, `UDP`, `ICMP` set
  - low severity for `ICMP` because it is allowed but treated as less common control-plane traffic

Each anomaly record includes:

- `source_row_id` when the warehouse row id exists
- `device_id`
- `event_timestamp`
- `rule_name`
- `severity`
- `reason`
- `score`

## Why the script uses Docker Compose and `psql`

Python's standard library does not include a PostgreSQL client like `psycopg2`. Stage 18A and 18B keep the script dependency-free, so they do not add a new package just for database access.

Instead, the script shells out to:

```powershell
docker compose exec -T postgres psql ...
```

The job runs that command from the repository root, reads CSV output from `psql`, and parses it with the Python standard library `csv` module. This approach is friendly to the current local Docker Compose workflow and works from Windows PowerShell because the script uses `subprocess.run(...)` with argument lists rather than shell-specific quoting.

## Local PostgreSQL environment variables

The script reads these environment variables and falls back to local defaults:

- `POSTGRES_HOST`, default `localhost`
- `POSTGRES_PORT`, default `5432`
- `POSTGRES_DB`, default `iot_logs`
- `POSTGRES_USER`, default `iot_user`
- `POSTGRES_PASSWORD`, default `iot_password`

Those values are forwarded to `psql` when the script queries `processed_iot_logs` and, if requested, inserts rows into `iot_anomalies`.

## Ensuring the `iot_anomalies` table exists

Stage 18B adds warehouse SQL file [`storage/postgres/init/04_create_iot_anomalies.sql`](../storage/postgres/init/04_create_iot_anomalies.sql).

On a fresh PostgreSQL Docker volume, that file can be picked up automatically from `/docker-entrypoint-initdb.d` during first-time initialization. If your local PostgreSQL volume already exists, use one of these safe options:

Run the SQL file manually through Docker Compose:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -f /docker-entrypoint-initdb.d/04_create_iot_anomalies.sql
```

Or let the anomaly detection script ensure the table:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --ensure-table --limit 100
```

The script uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`, so this step is idempotent.

## How to run Stage 18 locally

Validate Docker Compose first:

```powershell
docker compose config
```

Start PostgreSQL if it is not already running:

```powershell
docker compose up -d postgres
```

Compile-check the script:

```powershell
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\run_anomaly_detection.py
```

Run a local dry-run against the latest 100 rows and write a JSON report:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --dry-run --limit 100 --output-json docs/anomaly-detection-local.json
```

Run without DB writes but with the default generated `run_id`:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --limit 100 --output-json docs/anomaly-detection-local.json
```

Run with table creation check and warehouse persistence:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --limit 100 --ensure-table --write-db --output-json docs/anomaly-detection-local.json
```

Airflow integration now runs the same script automatically after `run_warehouse_loader` and before downstream dbt, Spark, MinIO, and observability tasks. The DAG task writes a git-ignored local summary to `docs/anomaly-detection-local.json` and persists anomalies into `iot_anomalies`.

Override the Airflow scan size by setting:

```powershell
$env:ANOMALY_DETECTION_LIMIT = "2500"
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

If you do not override it, the Airflow DAG uses `ANOMALY_DETECTION_LIMIT=1000`.

Verbose example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --limit 250 --verbose
```

## Stage 18 workflow

Stage 18 now works as one connected flow:

- `scripts/run_anomaly_detection.py` reads recent rows from `processed_iot_logs`
- matching rules produce one or more anomaly records per source row when needed
- `--write-db` persists those records into `iot_anomalies`
- Airflow task `run_anomaly_detection` runs the same logic after `run_warehouse_loader`
- repeated local runs append new rows with different `run_id` values instead of overwriting older results

For the full PR-ready Stage 18 explanation, portfolio framing, rule summary, and SQL inspection examples, see [docs/stage-18-anomaly-detection.md](./stage-18-anomaly-detection.md).

## What dry-run means

For Stage 18, dry-run means the job performs anomaly detection without attempting any database writes or pipeline mutations.

The script is still safe by default in the sense that it does not insert anomaly rows unless `--write-db` is passed. `--dry-run` is the strongest explicit no-write mode:

- it still reads recent rows from `processed_iot_logs`
- it still evaluates the rules
- it still prints the console summary
- it can still write an optional local JSON file
- it does not ensure tables
- it does not insert rows into `iot_anomalies`

## Writing anomalies into PostgreSQL

When `--write-db` is used, the script inserts each detected anomaly into `iot_anomalies`.

Important behavior:

- the default behavior remains read-only unless `--write-db` is passed
- each execution gets a `run_id`; you can provide one explicitly through `--run-id` or let the script generate a UTC timestamp-based value
- multiple anomaly rows can be created from one source `processed_iot_logs` row because a single event can match several rules at once

Example write command:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --limit 100 --ensure-table --write-db --run-id stage18b-validation --output-json docs/anomaly-detection-local.json
```

Airflow uses the same write path with an Airflow-derived run id shaped like `airflow-anomaly-...`, so anomaly rows from DAG runs can be isolated from manual runs through `run_id`.

## How to inspect inserted anomalies

Check how many anomaly rows exist:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM iot_anomalies;"
```

Check anomaly distribution by rule and severity:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT rule_name, severity, COUNT(*) FROM iot_anomalies GROUP BY rule_name, severity ORDER BY COUNT(*) DESC;"
```

Inspect one run id:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT run_id, source_row_id, device_id, rule_name, severity, score, created_at FROM iot_anomalies WHERE run_id = 'stage18b-validation' ORDER BY created_at DESC, id DESC LIMIT 20;"
```

Check the latest anomaly run quickly:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "WITH latest_run AS (SELECT run_id FROM iot_anomalies ORDER BY created_at DESC LIMIT 1) SELECT a.run_id, a.rule_name, a.severity, COUNT(*) AS anomaly_rows FROM iot_anomalies a JOIN latest_run lr ON a.run_id = lr.run_id GROUP BY a.run_id, a.rule_name, a.severity ORDER BY anomaly_rows DESC;"
```

## What the JSON output contains

When `--output-json` is provided, the script writes a local report that includes:

- generation timestamp
- `run_id`
- execution mode
- scan limit
- non-secret connection metadata
- summary counts
- full anomaly records

The summary includes:

- `rows_scanned`
- `anomalies_detected`
- `rows_with_anomalies`
- `anomalies_by_rule`
- `anomalies_by_severity`

The repository ignores the common local output path:

- `docs/anomaly-detection-local.json`

## Airflow integration in Stage 18C

Stage 18C adds task `run_anomaly_detection` to `airflow/dags/iot_local_pipeline_dag.py`.

Airflow behavior:

- runs after `run_warehouse_loader`
- runs before downstream analytical and quality-oriented tasks such as dbt, Spark, MinIO validation, and observability writing
- calls `scripts/run_anomaly_detection.py`
- passes `--ensure-table`
- passes `--write-db`
- passes `--limit` from `ANOMALY_DETECTION_LIMIT`, default `1000`
- writes local summary file `docs/anomaly-detection-local.json`
- uses an Airflow-derived `run_id` so DAG-created anomaly rows can be traced back to a run context

Because the summary path is already ignored by Git, repeated local Airflow runs can regenerate that JSON safely.

## How Stage 18A leads into Stage 18B and 18C

Stage 18A establishes the local detection logic and rule semantics first.

How the stage sequence now fits together:

- Stage 18A proves the rules locally
- Stage 18B persists anomaly results into additive warehouse storage for repeatable inspection
- Stage 18C orchestrates anomaly detection inside Airflow after the warehouse step and before downstream analytics and quality tasks

That sequencing keeps the repository safe:

- Stage 18A proves the rules locally
- Stage 18B adds warehouse visibility and run-level persistence
- Stage 18C adds orchestration only after the logic is stable
