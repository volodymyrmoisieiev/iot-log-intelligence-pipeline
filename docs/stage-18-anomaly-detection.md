# Stage 18 Anomaly Detection

Stage 18 adds a local anomaly-detection layer for processed IoT traffic. The goal is to show how curated warehouse data can be scanned for suspicious behavior with explainable rules before moving to heavier ML-style approaches.

## What Stage 18 adds

Stage 18 delivers three connected layers:

- a standalone local job at [`scripts/run_anomaly_detection.py`](../scripts/run_anomaly_detection.py)
- additive PostgreSQL persistence through table [`iot_anomalies`](../storage/postgres/init/04_create_iot_anomalies.sql)
- Airflow orchestration through task `run_anomaly_detection` inside [`airflow/dags/iot_local_pipeline_dag.py`](../airflow/dags/iot_local_pipeline_dag.py)

This means the repository can now:

- read recent curated rows from `processed_iot_logs`
- evaluate suspicious-traffic heuristics
- write anomaly records into `iot_anomalies`
- rerun detection safely with separate `run_id` values
- trigger the same logic from Airflow after warehouse loading

## Why rule-based anomaly detection is useful before ML

Rule-based anomaly detection is a strong first step before machine learning because it is:

- explainable, since every anomaly has a specific rule, reason, severity, and score
- cheap to validate locally, because it uses existing warehouse fields instead of a trained model
- easy to tune, because thresholds and suspicious-value lists can be adjusted without retraining
- useful for future ML, because rule hits can become labels, QA references, or baseline comparisons

For a data engineering portfolio, this shows practical thinking: establish a reliable detection foundation, persist results, orchestrate it, and only then consider more advanced modeling.

## Current rules

Stage 18 currently uses these rule families:

- `high_packet_size`
- `high_duration`
- `failed_or_blocked_status`
- `suspicious_attack_type`
- `unusual_protocol`

How they work in practice:

- high packet size flags unusually large `packet_size` values
- high duration flags unusually large `duration_ms` values
- failed or blocked status flags records with failure-style or blocked-style `status` values
- suspicious attack type flags non-benign `attack_type` values, with higher severity for known high-risk labels
- unusual protocol flags protocol values outside the expected set and can also mark less common but allowed traffic for lower-severity review

## How the job reads from `processed_iot_logs`

The anomaly job reads recent rows from warehouse table `processed_iot_logs`.

Because Stage 18 intentionally uses the Python standard library only, the script does not use `psycopg2`. Instead, it shells out to:

```powershell
docker compose exec -T postgres psql ...
```

It reads PostgreSQL query results as CSV, parses them in Python, and then applies the rule set row by row.

## How anomalies are written to `iot_anomalies`

When `--write-db` is passed, the script writes detected anomalies into `iot_anomalies`.

Each anomaly row stores:

- `run_id`
- `source_row_id`
- `event_timestamp`
- `device_id`
- `rule_name`
- `severity`
- `reason`
- `score`
- `created_at`

This keeps anomaly results additive rather than destructive. Repeated local runs do not overwrite previous results by default.

## Why one source row can create multiple anomaly records

One source record can match several rules at the same time.

For example, the same `processed_iot_logs` row might have:

- a large `packet_size`
- a long `duration_ms`
- a suspicious `attack_type`

In that case, the pipeline should preserve all of those signals instead of collapsing them into one generic flag. That is why Stage 18 stores anomaly records at the rule-hit level rather than one summary row per source event.

## Why `run_id` matters

`run_id` separates one detection execution from another.

This is important because repeated local validation runs append more anomaly rows. Instead of deleting previous results, the repository keeps them distinguishable by `run_id`, including:

- manual local runs such as `anomaly-detection-20260623T154352Z`
- Airflow task runs such as `airflow-anomaly-...`

That makes it easy to compare runs, troubleshoot one execution, or filter SQL queries to the most recent detection batch.

## Airflow integration

Stage 18C integrates anomaly detection into the local Airflow pipeline.

The DAG now runs anomaly detection:

- after `run_warehouse_loader`
- before downstream dbt, Spark, MinIO, and observability steps
- with `--ensure-table`
- with `--write-db`
- with `--limit` controlled by `ANOMALY_DETECTION_LIMIT`, default `1000`
- with a git-ignored JSON summary at `docs/anomaly-detection-local.json`

This keeps anomaly detection aligned with the existing local orchestration flow while preserving the standalone script for direct local use.

## How to inspect anomaly results with SQL

Check total anomaly rows:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM iot_anomalies;"
```

Check recent runs:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT run_id, COUNT(*) AS anomaly_rows FROM iot_anomalies GROUP BY run_id ORDER BY run_id DESC LIMIT 5;"
```

Inspect the latest run with rule distribution:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "WITH latest_run AS (SELECT run_id FROM iot_anomalies ORDER BY created_at DESC LIMIT 1) SELECT a.run_id, a.rule_name, a.severity, COUNT(*) AS anomaly_rows FROM iot_anomalies a JOIN latest_run lr ON a.run_id = lr.run_id GROUP BY a.run_id, a.rule_name, a.severity ORDER BY anomaly_rows DESC;"
```

Inspect detailed rows for one run:

```powershell
docker compose exec -T postgres psql -U iot_user -d iot_logs -c "SELECT run_id, source_row_id, device_id, rule_name, severity, score, reason, created_at FROM iot_anomalies WHERE run_id = 'airflow-anomaly-example' ORDER BY created_at DESC, id DESC LIMIT 20;"
```

## Limitations of rule-based detection

Rule-based anomaly detection is useful, but it has clear limitations:

- thresholds can be noisy if traffic patterns shift
- hand-written rules may miss subtle multi-event behaviors
- suspicious labels depend on current allowed-value assumptions
- the rules are only as good as the curated fields already present in `processed_iot_logs`
- append-only validation runs can grow local anomaly history over time unless you intentionally clean it outside the stage workflow

That is acceptable for this stage because the purpose is a stable, explainable detection foundation rather than a production-grade threat model.

## Portfolio value

For a Data Engineering portfolio, Stage 18 demonstrates more than simple scripting:

- warehouse-aware post-processing on curated pipeline output
- additive schema design for anomaly history
- standard-library-only data job design under dependency constraints
- repeatable local validation through Docker Compose and SQL
- orchestration through Airflow after warehouse loading
- run-scoped traceability through `run_id`

This shows end-to-end thinking: raw data becomes curated warehouse data, curated data becomes detection output, and detection output becomes queryable and orchestrated operational data.
