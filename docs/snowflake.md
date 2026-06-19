# Snowflake dbt Runbook

This runbook explains how to use the repository's optional Snowflake-ready dbt target safely.

## Warehouse strategy

- PostgreSQL is the default local and development warehouse.
- Docker-based dbt validation against PostgreSQL remains the primary local workflow.
- Snowflake support is optional and intended for future cloud warehouse integration.
- The dbt models have been reviewed to be more cross-database compatible, but Snowflake is still not required for local development or CI.

## Required Snowflake environment variables

Set these values through environment variables or a local `.env` file that is not committed:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_THREADS`

Placeholder values such as `your_snowflake_account` are examples only. They are useful for configuration shape and documentation, but they are not valid credentials.

## PostgreSQL validation through Docker

Use Docker by default for PostgreSQL dbt validation. The bundled dbt image already includes `dbt-postgres`.

```powershell
Set-Location "C:\Users\User\Desktop\Cloud Technologies\IoT_Log_Intelligence_Pipeline"
docker compose up -d postgres
docker compose run --build --rm dbt dbt debug
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose down
```

## Safe Snowflake validation

Use `dbt parse` as the safe Snowflake-ready validation step from the repository root. It validates project structure, profile selection, macros, and target-aware compilation without requiring a real Snowflake connection.

```powershell
Set-Location "C:\Users\User\Desktop\Cloud Technologies\IoT_Log_Intelligence_Pipeline"
dbt parse --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

This command is the recommended check when you only have placeholder Snowflake values configured.

## Snowflake commands only with real credentials

Run the commands below only when you have valid Snowflake credentials, understand the target account and warehouse, and are comfortable with the associated billing and execution behavior.

```powershell
Set-Location "C:\Users\User\Desktop\Cloud Technologies\IoT_Log_Intelligence_Pipeline"
dbt debug --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt run --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt test --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

Do not expect these commands to pass with placeholder values.

## Local adapter expectations

For local Windows dbt usage outside Docker:

- PostgreSQL commands need `dbt-postgres`.
- Snowflake commands need `dbt-snowflake`.
- If you want to run both targets from one local Python environment, install both adapters.

Docker should remain the default PostgreSQL validation path even if you also install local adapters.

## Troubleshooting

### `Could not find adapter type postgres`

This usually means your local Python environment has `dbt-snowflake` installed but does not have `dbt-postgres`.

Recommended response:

- use Docker for PostgreSQL dbt validation by default
- install `dbt-postgres` locally only if you intentionally want direct local PostgreSQL dbt commands

### Snowflake 404 with `your_snowflake_account`

This is expected when using placeholder values like `your_snowflake_account`. The placeholder profile is not a real Snowflake account, so connection-oriented commands such as `dbt debug`, `dbt run`, and `dbt test` will fail until real credentials and account-specific values are supplied.

### `Unable to do partial parsing`

This is not an error by itself. dbt prints this when it needs to rebuild its parse cache because the active config, profile, dependencies, or target changed.

## Safety

- Never commit Snowflake credentials, passwords, or tokens.
- Do not place secrets directly in `README.md` or other tracked docs.
- Use environment variables or a local `.env` file for credentials.
- Do not run real Snowflake `dbt debug`, `dbt run`, or `dbt test` commands unless credentials, access scope, and billing implications are understood.
- Snowflake remains optional in this project and is not required by CI.
