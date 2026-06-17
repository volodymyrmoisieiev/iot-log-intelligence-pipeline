# Airflow

Stage 7A adds the local Apache Airflow foundation for the IoT Log Intelligence Pipeline.

This stage is intentionally small in scope:

- it provisions a separate Airflow metadata PostgreSQL database
- it starts an Airflow webserver and scheduler locally with Docker Compose
- it includes a smoke DAG only to confirm that DAG discovery and task execution work
- it does not orchestrate the real producer, consumer, warehouse loader, dbt, or dashboard services yet

## Folder layout

```text
airflow/
|-- dags/
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

## Local access

- Airflow UI: [http://localhost:8081](http://localhost:8081/)
- Username: `airflow`
- Password: `airflow`

## Start Airflow

```bash
docker compose config
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
docker compose logs airflow-webserver --tail=50
docker compose logs airflow-scheduler --tail=50
```

## Smoke DAG check

After the services are up:

1. Open [http://localhost:8081](http://localhost:8081/).
2. Sign in with `airflow / airflow`.
3. Confirm that `iot_pipeline_smoke_dag` is visible in the DAG list.
4. Optionally trigger it manually to verify that simple tasks run successfully.

The smoke DAG is paused by default and has no schedule, so it is safe to keep in the repository without starting the real data pipeline.

## Stop services

```bash
docker compose stop airflow-webserver airflow-scheduler airflow-postgres
```

To remove the Airflow containers as well:

```bash
docker compose down
```
