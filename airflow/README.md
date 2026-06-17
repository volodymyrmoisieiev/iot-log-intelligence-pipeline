# Airflow

Stage 7B adds local Apache Airflow orchestration for the IoT Log Intelligence Pipeline.

This stage keeps the scope intentionally local:

- it preserves the separate Airflow metadata PostgreSQL database
- it keeps the Stage 7A smoke DAG for quick Airflow health checks
- it adds a manual orchestration DAG for the existing local Kafka, PostgreSQL, producer, consumer, warehouse-loader, and dbt steps
- it does not add Spark, AWS, Terraform, CI/CD, deployment logic, authentication, or real credentials
- it does not start the Streamlit dashboard from Airflow
- it uses a small custom Airflow image that adds Docker Compose support for local orchestration

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

For Stage 7B local orchestration, the Airflow services also mount:

- the full project repository at `/opt/project`
- the local Docker socket at `/var/run/docker.sock`

This is intentionally a local-development-only setup so `BashOperator` tasks can run `docker compose` against the existing repository services.

The custom Airflow image installs the Docker Compose plugin on top of the official Apache Airflow image so the orchestration DAG can reuse the repository's existing `docker compose` commands.

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

## Start Airflow

```bash
docker compose config
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

Tasks:

- `start`
- `check_airflow_environment`
- `finish`

### `iot_local_pipeline_dag`

Use this DAG to orchestrate the existing local data pipeline steps manually.

Task order:

- `start`
- `start_infrastructure`
- `run_go_producer`
- `run_python_consumer`
- `run_warehouse_loader`
- `run_dbt_run`
- `run_dbt_test`
- `finish`

What each orchestration task does:

- `start_infrastructure` starts Kafka, Kafka UI, topic initialization, and the warehouse PostgreSQL service
- `run_go_producer` runs the existing Go producer with zero send delay
- `run_python_consumer` processes a bounded batch of local Kafka messages
- `run_warehouse_loader` loads processed and invalid records into PostgreSQL
- `run_dbt_run` executes existing dbt models
- `run_dbt_test` executes existing dbt tests

The Streamlit dashboard is intentionally not started by this DAG.

## Trigger `iot_local_pipeline_dag`

After the services are up:

1. Open [http://localhost:8081](http://localhost:8081/).
2. Sign in with `airflow / airflow`.
3. Confirm that `iot_local_pipeline_dag` is visible in the DAG list.
4. Trigger the DAG manually from the Airflow UI.
5. Watch the tasks run in order from `start` through `finish`.

Both DAGs are paused by default and have no schedule, so they are safe for local development.

## Stop services

```bash
docker compose stop airflow-webserver airflow-scheduler airflow-postgres
```

To remove the Airflow containers as well:

```bash
docker compose down
```
