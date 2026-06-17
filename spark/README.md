# Spark

Stage 9A adds a local PySpark batch-processing foundation for the repository.

This layer is intentionally minimal:

- it runs PySpark in local mode inside Docker
- it provides a single smoke job to verify that Spark starts correctly
- it does not add real feature engineering, curated batch outputs, or production infrastructure

## What this layer is for

The Spark folder is a placeholder for future batch enrichment and large-scale transformations that can sit alongside the existing Kafka, PostgreSQL, dbt, Streamlit, and Airflow layers.

At this stage, Spark is only used to prove that:

- a local `SparkSession` can start
- a simple in-memory DataFrame can be processed
- a small aggregation can run successfully in Docker

## Files

- `Dockerfile`: local PySpark runtime image
- `requirements.txt`: minimal Python dependencies for Spark jobs
- `jobs/smoke_job.py`: simple local Spark smoke test

## Run the smoke job

From the repository root:

```bash
docker compose config
docker compose run --build --rm spark-batch
```

Expected behavior:

- Spark starts successfully
- the job creates a tiny in-memory DataFrame
- the job prints an aggregated result
- the process exits successfully

## Current limitations

- local PySpark only, not a Spark cluster
- no integration with Airflow yet
- no full batch feature engineering yet
- no AWS, EMR, Glue, S3, Terraform, Kubernetes, deployment, or secrets setup
