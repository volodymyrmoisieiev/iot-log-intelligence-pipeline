# Observability foundation

Stage 14A adds only the PostgreSQL schema foundation for pipeline observability. It does not change Airflow DAG behavior, dbt model logic, Kafka topics, dashboard behavior, or Terraform execution.

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

Stage 14 is about observability and data-quality alerting. Stage 14A intentionally stops at the database layer so later work can write audit rows, quality-check results, and alerts without first changing pipeline execution code.

This stage does not yet add:

- a quality monitor service
- Kafka alert publishing logic
- Airflow DAG changes
- dashboard changes

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

Validate the Compose file and PostgreSQL startup:

```powershell
docker compose config
docker compose up -d postgres
```

Apply the observability SQL file to an existing running volume if needed:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -f /docker-entrypoint-initdb.d/002_create_observability_tables.sql
```

Verify both the new observability tables and the existing warehouse tables:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('processed_iot_logs', 'invalid_iot_logs', 'pipeline_run_audit', 'pipeline_quality_checks', 'pipeline_alerts') ORDER BY tablename;"
```

Optional smoke-test inserts:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -c "INSERT INTO pipeline_run_audit (run_id, pipeline_name, environment, started_at, finished_at, status, processed_records, invalid_records, invalid_rate, high_risk_devices, total_alerts) VALUES ('stage14a-doc-test', 'iot_local_pipeline', 'local', NOW(), NOW(), 'success', 10, 1, 0.100000, 2, 1); INSERT INTO pipeline_quality_checks (run_id, check_name, check_status, severity, metric_name, metric_value, threshold_value, message) VALUES ('stage14a-doc-test', 'invalid_rate_threshold', 'pass', 'low', 'invalid_rate', 0.100000, 0.200000, 'Documentation validation row.'); INSERT INTO pipeline_alerts (run_id, alert_type, alert_level, alert_message, source, is_published_to_kafka) VALUES ('stage14a-doc-test', 'quality_threshold', 'info', 'Documentation validation row.', 'manual_validation', FALSE);"
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT run_id, status, processed_records, invalid_records FROM pipeline_run_audit WHERE run_id = 'stage14a-doc-test'; SELECT run_id, check_name, check_status, severity FROM pipeline_quality_checks WHERE run_id = 'stage14a-doc-test'; SELECT run_id, alert_type, alert_level, is_published_to_kafka FROM pipeline_alerts WHERE run_id = 'stage14a-doc-test';"
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -v ON_ERROR_STOP=1 -U iot_user -d iot_logs -c "DELETE FROM pipeline_alerts WHERE run_id = 'stage14a-doc-test'; DELETE FROM pipeline_quality_checks WHERE run_id = 'stage14a-doc-test'; DELETE FROM pipeline_run_audit WHERE run_id = 'stage14a-doc-test';"
```
