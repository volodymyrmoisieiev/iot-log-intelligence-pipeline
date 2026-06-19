# dbt

This directory contains the local PostgreSQL dbt foundation, staging layer, analytics marts, and an optional Snowflake-ready target template for future cloud warehouse integration.

## What is included

- a Docker image for running dbt locally through Docker Compose
- a dbt profile named `analytics`
- a default local PostgreSQL target named `dev`
- an optional Snowflake target named `snowflake`
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

The default local workflow remains PostgreSQL-based and should be run through Docker by default. The provided dbt Docker image already includes `dbt-postgres`, so it matches the repository's standard local warehouse workflow.

```powershell
docker compose run --build --rm dbt dbt debug
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
```

If you want to run dbt directly from a local Windows Python environment instead of Docker, install the adapters you need explicitly. For example, PostgreSQL commands need `dbt-postgres`, Snowflake commands need `dbt-snowflake`, and using both targets locally may require both adapters in the same environment.

## Optional Snowflake target

The `snowflake` target is included as a safe template for future cloud warehouse work. It is optional, it is not used by default, and CI does not require a real Snowflake connection.

For the full Snowflake runbook, troubleshooting notes, and safety guidance, see [docs/snowflake.md](../docs/snowflake.md).

Required environment variables for the Snowflake target:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_THREADS`

Install the Snowflake adapter only when you actually want to use that target:

```bash
pip install -r dbt/requirements-snowflake.txt
```

Safe repository-root syntax and config validation for the optional Snowflake target:

```powershell
dbt parse --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

Only with real Snowflake credentials:

```powershell
dbt debug --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt run --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt test --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

If you prefer to work inside the `dbt` directory instead, these commands are also correct:

```powershell
cd .\dbt
dbt parse --profiles-dir . --target snowflake
dbt debug --profiles-dir . --target snowflake
dbt run --profiles-dir . --target snowflake
dbt test --profiles-dir . --target snowflake
```

Placeholder values such as `your_snowflake_account` are examples only. `dbt parse` is a safe syntax/config validation step for that placeholder setup, but real `dbt debug`, `dbt run`, and `dbt test` checks against Snowflake are expected to fail until you provide valid Snowflake credentials and account-specific values through environment variables.

If you prefer Docker for PostgreSQL and a local Python environment for Snowflake, keep using Docker Compose for the default local target and only install the Snowflake adapter separately when needed.

## Notes

- The dbt container reads PostgreSQL settings from environment variables.
- The default `docker compose run --build --rm dbt ...` flow continues to use the local PostgreSQL target.
- Local Windows dbt commands may fail with `Could not find adapter type postgres` if the current Python environment has `dbt-snowflake` installed but not `dbt-postgres`.
- If you want to run both PostgreSQL and Snowflake targets directly from one local Python environment, install both `dbt-postgres` and `dbt-snowflake`.
- The optional Snowflake target uses environment variables only and must be selected explicitly with `--target snowflake`.
- From the repository root, Snowflake commands must include both `--project-dir .\dbt` and `--profiles-dir .\dbt`.
- For placeholder Snowflake values, prefer `dbt parse --project-dir .\dbt --profiles-dir .\dbt --target snowflake` as the safe local validation step.
- Snowflake credentials, passwords, tokens, and account-specific values must never be committed to the repository.
- `DBT_PROFILES_DIR` is expected to point to `/usr/app/dbt` inside the Docker container.
- Stage 5B adds analytics marts on top of Stage 5A staging models.
- Snowflake support here is configuration-only; no cloud deployment commands are introduced.
