# dbt

This directory contains the Stage 5B dbt foundation, staging layer, and analytics marts for the local PostgreSQL warehouse.

## What is included

- a Docker image for running dbt locally through Docker Compose
- a dbt profile named `analytics`
- source definitions for `processed_iot_logs` and `invalid_iot_logs`
- staging models:
  - `stg_processed_iot_logs`
  - `stg_invalid_iot_logs`
- analytics marts:
  - `mart_device_risk_summary`
  - `mart_attack_summary`
  - `mart_protocol_metrics`
  - `mart_pipeline_quality_summary`
- core data quality tests for required fields and allowed categorical values

## Run dbt locally

```bash
docker compose run --build --rm dbt dbt debug
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
```

## Notes

- The dbt container reads PostgreSQL settings from environment variables.
- `DBT_PROFILES_DIR` is expected to point to `/usr/app/dbt` inside the Docker container.
- Stage 5B adds analytics marts on top of Stage 5A staging models.
- Dashboards, Airflow, Spark, AWS, Terraform, CI/CD, and cloud deployment are still intentionally out of scope.
