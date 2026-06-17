# Spark

The `spark/` workspace contains the local PySpark batch-processing layer for the repository.

Current scope:

- Stage 9A adds the Dockerized local PySpark foundation and smoke job
- Stage 9B adds the first real device-level feature engineering batch job
- Spark runs locally through Docker, not as a multi-node Spark cluster

## What this layer is for

The Spark layer is reserved for future batch enrichment and larger-scale transformations that sit alongside the existing Kafka, PostgreSQL, dbt, Streamlit, and Airflow layers.

At the current stage, Spark provides:

- a smoke job that proves a local `SparkSession` can start and run
- a device feature engineering job that reads sample IoT logs and writes Parquet output

## Files

- `Dockerfile`: local PySpark runtime image
- `requirements.txt`: minimal Python dependencies for Spark jobs
- `jobs/smoke_job.py`: local Spark smoke test
- `jobs/device_features_job.py`: local device-level feature engineering job

## Smoke job purpose

`jobs/smoke_job.py` exists to verify the Stage 9A foundation only.

It:

- starts a local `SparkSession`
- creates a tiny in-memory DataFrame
- runs a simple aggregation
- prints the result
- stops Spark cleanly

Run it from the repository root:

```bash
docker compose config
docker compose run --build --rm spark-batch
```

Expected behavior:

- Spark starts successfully
- the job creates a tiny in-memory DataFrame
- the aggregation result is printed
- the process exits successfully

## Device features job purpose

`jobs/device_features_job.py` is the first real Stage 9B batch transformation.

It:

- reads sample IoT logs from `data/samples/sample_iot_logs.csv`
- validates that the required input columns exist
- casts `packet_size` and `duration_ms` to numeric types
- handles missing `attack_type`
- computes one output row per `device_id`
- writes the result to local Parquet in overwrite mode
- prints row counts, output path, and a preview
- stops Spark cleanly

## Input schema

The sample CSV currently uses these fields:

- `event_timestamp`
- `device_id`
- `source_ip`
- `destination_ip`
- `protocol`
- `packet_size`
- `duration_ms`
- `event_type`
- `attack_type`
- `status`

The feature job requires these columns:

- `event_timestamp`
- `device_id`
- `protocol`
- `packet_size`
- `duration_ms`
- `event_type`
- `attack_type`
- `status`

## Calculated features

The device feature engineering job writes these columns:

- `device_id`
- `total_events`
- `unique_protocols`
- `total_packet_size`
- `avg_packet_size`
- `max_packet_size`
- `avg_duration_ms`
- `max_duration_ms`
- `failed_events`
- `success_events`
- `failed_event_ratio`
- `attack_events`
- `attack_event_ratio`
- `first_event_timestamp`
- `last_event_timestamp`
- `risk_level`

## Risk level logic

- `high` if `attack_event_ratio >= 0.5` or `failed_event_ratio >= 0.5`
- `medium` if `attack_event_ratio > 0` or `failed_event_ratio > 0`
- otherwise `low`

## Input, output, and expected sample result

- input path: `data/samples/sample_iot_logs.csv`
- output path: `data/processed/spark/device_features`
- output format: Parquet

Expected sample-data result:

- `72` input rows
- `24` device-level feature rows

## Run commands

From the repository root:

```bash
docker compose config
docker compose run --build --rm spark-batch
docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py
docker compose run --rm spark-batch ls -la /app/data/processed/spark/device_features
```

## Current limitations

- local PySpark only, not a Spark cluster
- Spark is not integrated into Airflow yet
- Spark output is not loaded into PostgreSQL yet
- only one local device feature engineering job is implemented so far
- Spark does not use S3, EMR, Glue, Terraform, Kubernetes, deployment, or secrets setup
