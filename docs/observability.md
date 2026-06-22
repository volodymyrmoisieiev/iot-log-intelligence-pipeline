# Observability foundation

Stage 14C builds on the Stage 14A PostgreSQL schema foundation and Stage 14B local writer. It adds optional Kafka publishing for generated observability alerts while still leaving Airflow DAG behavior, dbt model logic, dashboard behavior, and Terraform execution unchanged.

## What these tables are for

The observability tables store pipeline-level telemetry that later stages can write and query:

- `pipeline_run_audit` stores one row per pipeline run with run timing, status, record counts, invalid-rate metrics, and alert totals.
- `pipeline_quality_checks` stores detailed quality-check outcomes for a run, including metric values, thresholds, severity, and human-readable messages.
- `pipeline_alerts` stores alert events that later stages may publish to Kafka or surface in dashboards.

These tables are additive. They do not replace or modify existing warehouse tables such as `processed_iot_logs` or `invalid_iot_logs`.

## Where initialization happens

The warehouse PostgreSQL service mounts [`storage/postgres/init`](../storage/postgres/init) into `/docker-entrypoint-initdb.d` through Docker Compose. On a fresh PostgreSQL volume, Docker automatically runs the SQL files in that directory during first-time database initialization.

Stage 14A adds [`storage/postgres/init/002_create_observability_tables.sql`](../storage/postgres/init/002_create_observability_tables.sql), which uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` so it is safe to re-run manually.

## How this fits Stage 14

Stage 14 is about observability and data-quality alerting.

- Stage 14A adds the additive PostgreSQL observability tables.
- Stage 14B adds a local Python writer that reads warehouse counts from `processed_iot_logs` and `invalid_iot_logs`, calculates `invalid_rate`, and writes audit rows, quality checks, and alerts.
- Stage 14C adds optional publishing of generated alert rows to Kafka topic `iot_pipeline_alerts`.

Stage 14C still does not add:

- a quality monitor service
- Airflow DAG changes
- dashboard changes

## Local writer

The Stage 14 writer lives at [`observability/write_pipeline_observability.py`](../observability/write_pipeline_observability.py).

It reads PostgreSQL connection settings from environment variables with local defaults:

- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=5432`
- `POSTGRES_DB=iot_logs`
- `POSTGRES_USER=iot_user`
- `POSTGRES_PASSWORD=iot_password`
- `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`
- `KAFKA_ALERT_TOPIC=iot_pipeline_alerts`

CLI arguments:

- `--run-id` required
- `--pipeline-name` default `iot_local_pipeline`
- `--environment` default `local`
- `--invalid-rate-threshold` default `0.20`
- `--min-processed-records` default `1`
- `--publish-alerts` optional
- `--kafka-bootstrap-servers` optional override for `KAFKA_BOOTSTRAP_SERVERS`
- `--kafka-alert-topic` optional override for `KAFKA_ALERT_TOPIC`

Writer behavior:

- inspects required PostgreSQL table schemas before writing
- counts rows in `processed_iot_logs`
- counts rows in `invalid_iot_logs`
- calculates `invalid_rate = invalid_records / (processed_records + invalid_records)`
- writes one `pipeline_run_audit` row per `run_id`
- rewrites `pipeline_quality_checks` rows for the same `run_id`
- rewrites `pipeline_alerts` rows for the same `run_id`
- keeps `pipeline_alerts.is_published_to_kafka = false` unless Kafka publishing succeeds
- optionally publishes generated alerts to Kafka as JSON when `--publish-alerts` is used
- uses one transaction and rolls back everything on failure

Stage 14B currently stores `high_risk_devices` as `0` because this writer only reads the already-established warehouse row counts and invalid-rate metric. Later observability stages can expand that metric when a concrete warehouse rule is defined.

Kafka publishing note:

- PostgreSQL rows remain idempotent for the same `run_id`
- Kafka is append-only, so repeated validation with `--publish-alerts` can produce additional topic messages even when the database still contains only one logical set of rows for that `run_id`

## Apply to an existing local PostgreSQL volume

If your local `postgres_data` volume already exists, Docker will not replay `/docker-entrypoint-initdb.d` automatically. In that case, keep the existing volume and run the observability SQL file manually against the running container:

```powershell
docker compose up -d postgres
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -f /docker-entrypoint-initdb.d/002_create_observability_tables.sql
```

This applies the new tables without deleting warehouse data.

If you changed the PostgreSQL database name, user, or password from the defaults in `.env.example`, adjust those values in the command above.

## Volume safety note

Avoid `docker compose down -v` unless you intentionally want to delete Docker volumes and lose local database state. For this repository, that can remove existing warehouse tables, sample pipeline results, and other local validation data stored in the named PostgreSQL volumes.

Use `docker compose down` when you only want to stop containers and keep data.

## PowerShell validation

Validate the Compose file and PostgreSQL/Kafka startup:

```powershell
docker compose config
docker compose up -d kafka kafka-ui kafka-init postgres
```

Apply the observability SQL file to an existing running volume if needed:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -f /docker-entrypoint-initdb.d/002_create_observability_tables.sql
```

Install the local writer dependency:

```powershell
python -m pip install -r .\observability\requirements.txt
```

Run the writer without Kafka publishing:

```powershell
python .\observability\write_pipeline_observability.py --run-id stage14b-validation
```

Run the writer with Kafka publishing enabled:

```powershell
python .\observability\write_pipeline_observability.py --run-id stage14c-validation --publish-alerts --min-processed-records 999999
```

Verify both the new observability tables and the existing warehouse tables:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('processed_iot_logs', 'invalid_iot_logs', 'pipeline_run_audit', 'pipeline_quality_checks', 'pipeline_alerts') ORDER BY tablename;"
```

Inspect the observability rows written for one run id:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT run_id, status, processed_records, invalid_records, invalid_rate, total_alerts FROM pipeline_run_audit WHERE run_id = 'stage14c-validation'; SELECT run_id, check_name, check_status, severity, metric_name, metric_value, threshold_value FROM pipeline_quality_checks WHERE run_id = 'stage14c-validation' ORDER BY check_name; SELECT run_id, alert_type, alert_level, is_published_to_kafka FROM pipeline_alerts WHERE run_id = 'stage14c-validation' ORDER BY alert_type;"
```

Force an alert for validation:

```powershell
python .\observability\write_pipeline_observability.py --run-id stage14c-validation --publish-alerts --min-processed-records 999999
```

Consume one alert message from Kafka topic `iot_pipeline_alerts`:

```powershell
docker exec -i iot-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_pipeline_alerts --from-beginning --max-messages 1
```

Run the writer again with the same run id to prove database idempotency:

```powershell
python .\observability\write_pipeline_observability.py --run-id stage14c-validation --publish-alerts --min-processed-records 999999
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS audit_rows FROM pipeline_run_audit WHERE run_id = 'stage14c-validation'; SELECT COUNT(*) AS quality_rows FROM pipeline_quality_checks WHERE run_id = 'stage14c-validation'; SELECT COUNT(*) AS alert_rows FROM pipeline_alerts WHERE run_id = 'stage14c-validation';"
```

Clean up only validation rows from PostgreSQL observability tables:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -c "DELETE FROM pipeline_alerts WHERE run_id = 'stage14c-validation'; DELETE FROM pipeline_quality_checks WHERE run_id = 'stage14c-validation'; DELETE FROM pipeline_run_audit WHERE run_id = 'stage14c-validation';"
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS audit_rows FROM pipeline_run_audit WHERE run_id = 'stage14c-validation'; SELECT COUNT(*) AS quality_rows FROM pipeline_quality_checks WHERE run_id = 'stage14c-validation'; SELECT COUNT(*) AS alert_rows FROM pipeline_alerts WHERE run_id = 'stage14c-validation';"
```

Do not delete Kafka topic data during validation. Re-running the same validation may append extra Kafka messages for the same logical `run_id`, which is expected for an append-only topic.
