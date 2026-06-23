# Airflow

Stage 7 covers the local Apache Airflow foundation and orchestration layer for the IoT Log Intelligence Pipeline.

This Airflow setup is intentionally local-development only:

- it preserves the separate Airflow metadata PostgreSQL database
- it keeps the Stage 7A smoke DAG for quick Airflow health checks
- it keeps the manual orchestration DAG for the existing local Kafka, PostgreSQL, producer, consumer, warehouse-loader, dbt, and Spark steps
- it adds Kafka reset and warehouse truncate steps for safer repeatable demo runs
- it validates the selected raw input CSV against the Stage 17 data contract before producer execution
- it generates unique Kafka consumer and loader group ids per Airflow run
- it runs Spark through the existing local `spark-batch` Docker Compose service in local Docker mode only
- it uploads Spark device features to local MinIO and validates uploaded objects through the existing local object-storage services
- it runs anomaly detection after warehouse loading and persists anomaly rows into PostgreSQL
- it runs the observability writer after the pipeline outputs are available and validates observability rows in PostgreSQL
- it does not add AWS, Terraform, CI/CD, deployment logic, authentication, or real credentials
- it does not start the Streamlit dashboard from Airflow
- it uses a small custom Airflow image that adds Docker Compose support for local orchestration
- it keeps `sample` dataset mode as the safe default and lets you override dataset-mode settings through environment variables before starting Airflow

## What Stage 7 does

Stage 7 gives this repository a local orchestration layer so you can:

- bring up Airflow with Docker Compose
- verify Airflow itself with a smoke DAG
- trigger the existing local data pipeline from the Airflow UI
- rerun the pipeline more safely for demos by resetting local Kafka runtime state and truncating only the warehouse pipeline tables
- persist rule-based anomalies into `iot_anomalies` after warehouse loading
- run the PySpark device feature engineering job after dbt, validate that Parquet output exists, upload that output to local MinIO, validate uploaded objects, write observability metrics, and validate observability rows

## Folder layout

```text
airflow/
|-- dags/
|   |-- iot_local_pipeline_dag.py
|   `-- iot_pipeline_smoke_dag.py
|-- logs/
|   `-- .gitkeep
|-- plugins/
|   `-- .gitkeep
`-- README.md
```

## Local Airflow services

Airflow uses these Docker Compose services:

- `airflow-postgres` for the Airflow metadata database
- `airflow-init` to initialize metadata tables and create the local admin user
- `airflow-webserver` for the UI
- `airflow-scheduler` for DAG parsing and task scheduling

The metadata database is separate from the project warehouse PostgreSQL service.

Why keep them separate:

- Airflow needs its own metadata tables for DAG runs, task history, users, and scheduling state
- the warehouse PostgreSQL service stores pipeline data such as `processed_iot_logs` and `invalid_iot_logs`
- separating them keeps orchestration state isolated from analytics data and makes local troubleshooting safer

For Stage 7C local orchestration, the Airflow services also mount:

- the full project repository at `/opt/project`
- the local Docker socket at `/var/run/docker.sock`

This is intentionally a local-development-only setup so `BashOperator` tasks can run `docker compose` against the existing repository services.

The custom Airflow image installs the Docker Compose plugin on top of the official Apache Airflow image so the orchestration DAG can reuse the repository's existing `docker compose` commands.

Why the Docker socket is mounted:

- `iot_local_pipeline_dag` calls the repository's existing `docker compose` commands from inside Airflow tasks
- the Airflow container therefore needs access to the local Docker daemon
- this is useful for local orchestration only and should not be treated as a production deployment pattern

## Local path requirement

The orchestration DAG needs one local path variable so Docker Compose running inside Airflow can resolve host bind mounts correctly on Windows:

- `HOST_PROJECT_ROOT`

Use a forward-slash absolute host path, for example:

```bash
HOST_PROJECT_ROOT=C:/Users/User/Desktop/Cloud Technologies/IoT_Log_Intelligence_Pipeline
```

The current Docker Compose file provides a local default for this repository path, but if you move the project to another location you should override `HOST_PROJECT_ROOT` in your local environment or `.env`.

## Local access

- Airflow UI: [http://localhost:8081](http://localhost:8081/)
- Username: `airflow`
- Password: `airflow`

Related local ports:

- Kafka UI: `http://localhost:8080`
- Airflow UI: `http://localhost:8081`
- Streamlit dashboard: `http://localhost:8501`
- PostgreSQL: `localhost:5432`

## Start Airflow

```bash
docker compose config
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags list
```

Expected DAGs:

- `iot_pipeline_smoke_dag`
- `iot_local_pipeline_dag`

## Open Airflow UI

1. Open [http://localhost:8081](http://localhost:8081/).
2. Sign in with `airflow / airflow`.

## DAGs in this stage

### `iot_pipeline_smoke_dag`

Use this DAG for a quick Airflow check only.

Purpose:

- verify that Airflow loads DAGs
- verify that a simple task can execute
- avoid touching Kafka, PostgreSQL warehouse data, dbt, or the dashboard

Tasks:

- `start`
- `check_airflow_environment`
- `finish`

### `iot_local_pipeline_dag`

Use this DAG to orchestrate the existing local data pipeline steps manually with safer repeated-run behavior.

Purpose:

- run the local Kafka -> consumer -> warehouse -> dbt -> Spark flow from the Airflow UI
- make demo reruns more consistent
- keep orchestration logic in Airflow without rewriting the producer, consumer, warehouse-loader, dbt, or Spark services
- stop the pipeline early if the selected raw CSV breaks the expected contract

Dataset-mode defaults in this DAG:

- `DATASET_PROFILE=sample`
- `PRODUCER_MAX_ROWS=0`
- `PRODUCER_SEND_DELAY_MS=0`
- `CONSUMER_MAX_MESSAGES=72`
- `WAREHOUSE_LOADER_MAX_MESSAGES=72`
- `CONSUMER_PROGRESS_INTERVAL=1000`
- `WAREHOUSE_LOADER_PROGRESS_INTERVAL=1000`
- `ANOMALY_DETECTION_LIMIT=1000`

These defaults keep the existing local DAG behavior sample-safe. You can override them in your shell before starting Airflow when you intentionally want a `medium` validation run.

Task order:

- `start`
- `reset_local_pipeline_state`
- `start_infrastructure`
- `truncate_warehouse_tables`
- `validate_raw_data_contract`
- `run_go_producer`
- `run_python_consumer`
- `run_warehouse_loader`
- `run_anomaly_detection`
- `run_dbt_run`
- `run_dbt_test`
- `run_spark_device_features`
- `validate_spark_device_features_output`
- `start_object_storage`
- `upload_spark_features_to_minio`
- `validate_minio_spark_features_upload`
- `run_observability_writer`
- `validate_observability_output`
- `finish`

What each orchestration task does:

- `reset_local_pipeline_state` stops and removes only Kafka runtime containers so the next run starts with fresh local Kafka state
- `start_infrastructure` starts Kafka, Kafka UI, topic initialization, and the warehouse PostgreSQL service
- `truncate_warehouse_tables` clears only `processed_iot_logs` and `invalid_iot_logs` with `RESTART IDENTITY`
- `validate_raw_data_contract` runs `scripts/validate_data_contract.py` inside the Airflow container before producer execution and fails the DAG immediately if the selected CSV does not satisfy `contracts/iot_raw_log_contract.yml`
- `run_go_producer` runs the existing Go producer with zero send delay
- `run_go_producer` also passes `DATASET_PROFILE`, `PRODUCER_MAX_ROWS`, and `PRODUCER_SEND_DELAY_MS`
- `run_python_consumer` processes a bounded batch of local Kafka messages with a unique consumer group id per DAG run and passes `CONSUMER_MAX_MESSAGES` plus `CONSUMER_PROGRESS_INTERVAL`
- `run_warehouse_loader` loads processed and invalid records into PostgreSQL with a unique loader group id per DAG run and passes `WAREHOUSE_LOADER_MAX_MESSAGES` plus `WAREHOUSE_LOADER_PROGRESS_INTERVAL`
- `run_anomaly_detection` runs `scripts/run_anomaly_detection.py` inside the Airflow container after warehouse loading, uses `--ensure-table` plus `--write-db`, persists results into `iot_anomalies`, writes `docs/anomaly-detection-local.json`, and passes an Airflow-derived run id together with `ANOMALY_DETECTION_LIMIT`
- `run_dbt_run` executes existing dbt models
- `run_dbt_test` executes existing dbt tests
- `run_spark_device_features` runs `python /app/jobs/device_features_job.py` through `spark-batch`
- `validate_spark_device_features_output` confirms `data/processed/spark/device_features` exists and contains at least one Parquet file
- `start_object_storage` starts local MinIO and runs bucket initialization through the `object-storage` Docker Compose profile
- `upload_spark_features_to_minio` runs the existing `object-storage-uploader` service to upload Spark Parquet output into bucket `iot-data-lake`
- `validate_minio_spark_features_upload` uses the MinIO client to confirm at least one uploaded `.parquet` object exists under `spark/device_features/latest/`
- `run_observability_writer` runs the Dockerized observability writer with an Airflow-derived run id and `--publish-alerts`
- `validate_observability_output` checks for exactly one audit row and at least two quality-check rows for that observability run id

The Streamlit dashboard is intentionally not started by this DAG.
Airflow metadata tables are intentionally not truncated or reset by this DAG.
Spark output remains local Parquet output that is uploaded into local MinIO only; it is not loaded into PostgreSQL by Airflow and it is not sent to production AWS S3.
Kafka alert messages remain append-only, so repeated DAG runs create separate observability run ids and may append additional topic messages.
The anomaly detection JSON summary path `docs/anomaly-detection-local.json` is git-ignored and can be regenerated safely during repeated local DAG runs.

Raw-contract input path mapping inside Airflow uses `/opt/project`:

- `sample` -> `/opt/project/data/samples/sample_iot_logs.csv`
- `medium` -> `/opt/project/data/processed/medium_iot_logs.csv`
- `full` -> `/opt/project/data/raw/full_iot_logs.csv`

The validator writes a local summary file to `docs/data-contract-validation-local.json`. That artifact is git-ignored and can be regenerated safely during local Airflow runs.

## What repeatable runs reset

`iot_local_pipeline_dag` resets only local demo state that is safe to refresh:

- Kafka containers: `kafka`, `kafka-init`, and `kafka-ui`
- warehouse pipeline tables:
  - `processed_iot_logs`
  - `invalid_iot_logs`
- Kafka consumer group ids by generating unique names from each Airflow `run_id`

## What repeatable runs do not reset

`iot_local_pipeline_dag` does not:

- reset the Airflow metadata database
- stop the Airflow webserver
- stop the Airflow scheduler
- run `docker compose down -v`
- remove the warehouse PostgreSQL volume
- drop the warehouse database
- delete repository source files
- start the Streamlit dashboard

## Trigger `iot_local_pipeline_dag`

After the services are up:

1. Open [http://localhost:8081](http://localhost:8081/).
2. Sign in with `airflow / airflow`.
3. Confirm that `iot_local_pipeline_dag` is visible in the DAG list.
4. Trigger the DAG manually from the Airflow UI.
5. Watch the tasks run in order from `start` through `finish`.

For repeatability checks, trigger the DAG a second time after the first run succeeds. The Kafka reset step, warehouse truncate step, and unique run-scoped group ids make repeated local demo runs more consistent.

Both DAGs are paused by default and have no schedule, so they are safe for local development.

Expected successful local result:

- `validate_raw_data_contract` succeeds before the producer starts
- Go producer sends `72` records
- Python consumer processes `72` records
- warehouse-loader inserts `72` processed records
- `dbt run` succeeds
- `dbt test` succeeds with `53` tests
- `run_spark_device_features` writes Parquet output to `data/processed/spark/device_features`
- `validate_spark_device_features_output` succeeds after finding at least one Parquet file
- `start_object_storage` brings up local MinIO and bucket initialization successfully
- `upload_spark_features_to_minio` uploads Spark Parquet output to `iot-data-lake`
- `validate_minio_spark_features_upload` succeeds after finding at least one uploaded `.parquet` object under `spark/device_features/latest/`
- `run_anomaly_detection` succeeds and writes anomaly rows into `iot_anomalies`
- `run_observability_writer` succeeds
- `validate_observability_output` succeeds

## Dataset mode runbook

### Default sample-mode Airflow run

If you do not set any dataset-specific overrides, `iot_local_pipeline_dag` stays on the tracked sample dataset and behaves like the current demo pipeline.

Recommended sample-mode values:

- `DATASET_PROFILE=sample`
- `PRODUCER_MAX_ROWS=0`
- `CONSUMER_MAX_MESSAGES=72`
- `WAREHOUSE_LOADER_MAX_MESSAGES=72`
- `CONSUMER_PROGRESS_INTERVAL=1000`
- `WAREHOUSE_LOADER_PROGRESS_INTERVAL=1000`
- `ANOMALY_DETECTION_LIMIT=1000`

### Prepare a medium dataset

```powershell
& 'C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe' .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

For a lighter local validation pass, you can use a smaller row target such as `--rows 500` or `--rows 1000`.

### Run Airflow with medium profile

Set the overrides before starting the Airflow services:

```powershell
$env:DATASET_PROFILE = "medium"
$env:PRODUCER_MAX_ROWS = "1000"
$env:CONSUMER_MAX_MESSAGES = "1000"
$env:WAREHOUSE_LOADER_MAX_MESSAGES = "1000"
$env:CONSUMER_PROGRESS_INTERVAL = "250"
$env:WAREHOUSE_LOADER_PROGRESS_INTERVAL = "250"
$env:ANOMALY_DETECTION_LIMIT = "1000"
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Before a fuller DAG validation that includes `run_spark_device_features`, prebuild the Spark image once so Airflow does not rebuild it during the DAG run:

```powershell
docker compose build spark-batch
```

Recommended medium-mode guidance:

- keep `PRODUCER_MAX_ROWS`, `CONSUMER_MAX_MESSAGES`, and `WAREHOUSE_LOADER_MAX_MESSAGES` aligned so the consumer and loader expect the same number of records the producer will send
- increase `ANOMALY_DETECTION_LIMIT` only when you intentionally want the DAG to scan more warehouse rows for anomaly persistence
- use unique Airflow-triggered run ids as usual; the DAG already derives unique Kafka group ids from `run_id`
- keep `full` dataset mode for manual local or cloud-style validation only, not CI

### Useful Airflow commands

List DAGs:

```powershell
docker compose run --rm airflow-webserver airflow dags list
```

List DAG tasks:

```powershell
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
```

Trigger the DAG manually from CLI:

```powershell
docker compose run --rm airflow-webserver airflow dags trigger iot_local_pipeline_dag
```

Test only the anomaly task:

```powershell
docker compose run --rm airflow-webserver airflow tasks test iot_local_pipeline_dag run_anomaly_detection 2024-01-01
```

Validate row counts after a run:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS processed_rows FROM processed_iot_logs;"
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS invalid_rows FROM invalid_iot_logs;"
```

For sample-mode validation, you usually expect `72` processed rows and `0` invalid rows.
For medium-mode validation, expected row counts should match the producer row cap you intentionally configured.

## Troubleshooting

### Airflow UI does not open

- confirm `docker compose ps` shows `airflow-webserver` running
- check `docker compose logs airflow-webserver --tail=80`
- confirm you are opening [http://localhost:8081](http://localhost:8081/) and not Kafka UI on port `8080`

### Webserver health is still starting

- wait a little longer after `docker compose up`
- recheck with `docker compose ps`
- inspect `docker compose logs airflow-webserver --tail=80`

### Docker socket permission issue

- confirm `/var/run/docker.sock` is mounted into the Airflow services
- confirm the local Docker daemon is running
- rerun `docker compose exec airflow-webserver docker compose version`

### `docker compose` not found inside Airflow container

- rebuild the Airflow images:

```bash
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

- the custom Airflow image must include `docker-compose-plugin`

### DAG does not appear in the UI

- run `docker compose exec airflow-webserver airflow dags list`
- check `docker compose exec airflow-webserver airflow dags show iot_local_pipeline_dag`
- run `docker compose exec airflow-webserver airflow tasks list iot_local_pipeline_dag`
- review `docker compose logs airflow-scheduler --tail=80`

### DAG task fails because previous containers are still running

- `reset_local_pipeline_state` is designed to stop and remove Kafka runtime containers before the next run
- if a run was interrupted mid-flight, trigger the DAG again so the reset step executes cleanly

### Raw data contract validation task fails

- confirm the selected dataset file exists for the active `DATASET_PROFILE`
- sample path: `/opt/project/data/samples/sample_iot_logs.csv`
- medium path: `/opt/project/data/processed/medium_iot_logs.csv`
- full path: `/opt/project/data/raw/full_iot_logs.csv`
- run `docker compose run --rm airflow-webserver airflow tasks test iot_local_pipeline_dag validate_raw_data_contract 2024-01-01`
- inspect the local summary artifact at `docs/data-contract-validation-local.json`
- rerun the validator directly with `.\.venv-observability\Scripts\python.exe .\scripts\validate_data_contract.py --input .\data\samples\sample_iot_logs.csv --summary-json docs/data-contract-validation-local.json`

### Spark validation task fails

- confirm `run_spark_device_features` succeeded earlier in the same DAG run
- inspect `data/processed/spark/device_features` for `part-*.parquet` or other `.parquet` files
- run `docker compose build spark-batch` first if the Spark image has not been built yet
- rerun `docker compose run --rm spark-batch python /app/jobs/device_features_job.py` if you need to verify the Spark job independently
- remember that the validation task checks output existence only, not full data quality

### MinIO upload or validation task fails

- confirm `start_object_storage` succeeded earlier in the same DAG run
- confirm bucket `iot-data-lake` exists in the MinIO console at [http://localhost:9001](http://localhost:9001/)
- rerun `docker compose --profile object-storage run --build --rm object-storage-uploader` to verify the uploader independently
- rerun `docker compose --profile object-storage run --rm --entrypoint /bin/sh minio-init -ec 'mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls --recursive local/$MINIO_BUCKET/spark/device_features/latest/'` to verify uploaded objects independently
- remember that this flow targets local MinIO only, not production AWS S3

### Observability writer or validation task fails

- rerun `docker compose run --build --rm observability-writer --run-id manual-observability-check --publish-alerts`
- inspect PostgreSQL audit rows with `docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT run_id, status, processed_records, invalid_records, invalid_rate, total_alerts FROM pipeline_run_audit WHERE run_id LIKE 'airflow-observability-%' ORDER BY created_at DESC LIMIT 5;"`
- inspect quality-check rows with `docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT run_id, check_name, check_status, severity FROM pipeline_quality_checks WHERE run_id LIKE 'airflow-observability-%' ORDER BY created_at DESC LIMIT 10;"`
- if alerts were generated, remember topic `iot_pipeline_alerts` is append-only and repeated runs may add more Kafka messages

### Anomaly detection task fails

- run `docker compose run --rm airflow-webserver airflow tasks test iot_local_pipeline_dag run_anomaly_detection 2024-01-01`
- rerun the script directly with `.\.venv-observability\Scripts\python.exe .\scripts\run_anomaly_detection.py --limit 100 --ensure-table --write-db --output-json docs/anomaly-detection-local.json`
- inspect anomaly rows with `docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT run_id, rule_name, severity, COUNT(*) FROM iot_anomalies GROUP BY run_id, rule_name, severity ORDER BY run_id DESC, COUNT(*) DESC;"`
- confirm `ANOMALY_DETECTION_LIMIT` is not accidentally set to an unexpected value before starting Airflow

### Kafka offsets or repeated runs are confusing

- Stage 7C uses unique consumer and loader group ids per `run_id`
- Kafka runtime containers are reset before each local pipeline run
- warehouse tables are truncated before data is reloaded

### Windows `HOST_PROJECT_ROOT` path issue

- use a forward-slash absolute path such as `C:/Users/User/Desktop/Cloud Technologies/IoT_Log_Intelligence_Pipeline`
- if the repository moves, update `HOST_PROJECT_ROOT` in your local environment or `.env`
- bind mounts inside Airflow depend on this path being correct

### Port conflicts

- Kafka UI uses `8080`
- Airflow UI uses `8081`
- Streamlit uses `8501`
- PostgreSQL uses `5432`

If one of these ports is already occupied, free the port or adjust your local port mapping before starting the stack.

## Final verification checklist

Run:

```bash
docker compose config
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags show iot_local_pipeline_dag
docker compose run --rm airflow-webserver python -m py_compile /opt/airflow/dags/iot_local_pipeline_dag.py
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
docker compose run --rm airflow-webserver airflow tasks test iot_local_pipeline_dag validate_raw_data_contract 2024-01-01
docker compose run --rm airflow-webserver airflow tasks test iot_local_pipeline_dag run_anomaly_detection 2024-01-01
docker compose run --build --rm observability-writer --run-id airflow-readme-validation --publish-alerts
```

Manual UI verification:

- open [http://localhost:8081](http://localhost:8081/)
- log in with `airflow / airflow`
- confirm both DAGs are visible
- trigger `iot_local_pipeline_dag`
- confirm `validate_raw_data_contract` succeeds before `run_go_producer`
- confirm every task reaches `success`
- confirm `run_anomaly_detection` succeeds after `run_warehouse_loader`
- confirm `run_dbt_test` reports `53` passing tests
- confirm `run_spark_device_features` succeeds
- confirm `validate_spark_device_features_output` succeeds
- confirm `start_object_storage` succeeds
- confirm `upload_spark_features_to_minio` succeeds
- confirm `validate_minio_spark_features_upload` succeeds
- confirm `data/processed/spark/device_features` contains Parquet output
- confirm MinIO bucket `iot-data-lake` contains uploaded Parquet objects under `spark/device_features/latest/`

## Stop services

```bash
docker compose stop airflow-webserver airflow-scheduler airflow-postgres
```

To remove the Airflow containers as well:

```bash
docker compose down
```
