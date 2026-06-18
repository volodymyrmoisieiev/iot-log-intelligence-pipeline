# IoT Log Intelligence Pipeline

## 1. Project overview

IoT Log Intelligence Pipeline is a portfolio project focused on end-to-end data engineering for IoT logs: ingestion, processing, storage, transformation, and analytics.

The repository is currently at Stage 11A, with a working local Kafka stack, a Go producer, a Python consumer validation layer, a local PostgreSQL warehouse foundation, a warehouse loader service, dbt staging models, dbt analytics marts on top of PostgreSQL, a polished Streamlit dashboard for local analytics, safer repeatable local Apache Airflow orchestration for the existing pipeline steps, a lightweight GitHub Actions CI workflow for repository validation, tests, dbt project validation, and Airflow DAG validation, a local PySpark batch-processing foundation with a device-level feature engineering job that runs inside the local Airflow pipeline, and a local MinIO-based S3-compatible object storage foundation for future data lake work.

## 2. Planned local architecture

```text
Raw logs -> Go Producer -> Kafka -> Python Consumer -> Kafka processed/invalid topics -> warehouse loader -> PostgreSQL -> dbt -> Streamlit dashboard
```

Local MVP focus:

- Ingest or simulate IoT log events.
- Publish events with a Go producer.
- Consume and validate them in Python.
- Store curated data in a local warehouse or data lake layer.
- Provision a local S3-compatible object storage foundation for future lake workflows.
- Model analytics-ready datasets with dbt and SQL.
- Explore results in a Streamlit dashboard.

## 3. Planned AWS architecture

```text
S3 Bronze -> Lambda -> Step Functions -> processing -> S3 Silver/Gold -> warehouse -> dbt -> CloudWatch
```

AWS advanced focus:

- Land raw logs in S3 Bronze.
- Use Lambda for lightweight preprocessing and event handling.
- Coordinate multi-step flows with Step Functions.
- Promote cleaned data into S3 Silver and Gold.
- Load curated data into Snowflake or BigQuery.
- Track jobs and platform health with CloudWatch.

## 4. Tech stack

- Go
- Kafka
- Python
- PySpark
- Airflow
- Snowflake or BigQuery
- dbt
- SQL
- Streamlit
- Docker
- MinIO
- Terraform
- AWS S3
- AWS Lambda
- AWS Step Functions
- AWS CloudWatch
- GitHub Actions

## 5. Repository structure

```text
iot-log-intelligence-pipeline/
|-- .github/workflows/
|-- airflow/dags/
|-- aws/
|   |-- lambda/
|   `-- step-functions/
|-- dashboard/
|-- data/
|   |-- raw/
|   |-- processed/
|   `-- samples/
|-- dbt/models/
|   |-- staging/
|   |-- intermediate/
|   `-- marts/
|-- docs/
|   `-- screenshots/
|-- go-producer/
|-- infra/terraform/
|   |-- modules/
|   |   |-- cloudwatch/
|   |   |-- iam/
|   |   |-- lambda/
|   |   |-- s3/
|   |   `-- step_functions/
|   `-- environments/
|       |-- dev/
|       `-- prod/
|-- python-consumer/
|-- spark/
|-- storage/postgres/init/
|-- sql/
|-- tests/
|-- warehouse-loader/
|-- .env.example
|-- .gitignore
|-- docker-compose.yml
`-- README.md
```

## 6. Roadmap

- Stage 0: repository setup
- Stage 1: Docker + Kafka
- Stage 2: Go producer
- Stage 3: Python consumer
- Stage 4: local storage
- Stage 5: dbt + SQL metrics
- Stage 6: Streamlit dashboard
- Stage 7: Airflow
- Stage 8A: basic CI quality automation
- Stage 8B: dbt validation in CI
- Stage 8C: Airflow DAG validation in CI
- Stage 9A: PySpark batch foundation
- Stage 9B: batch feature engineering
- Stage 10: Airflow + PySpark integration
- Stage 11A: local S3-compatible data lake foundation
- Stage 11B: AWS + Terraform
- Stage 12: CD + final docs

## 7. Stage 1 local setup

Stage 1 brings up Kafka locally with Docker Compose, exposes Kafka UI at [http://localhost:8080](http://localhost:8080/), and creates the required topics automatically for future pipeline stages.

Run the local stack:

```bash
docker compose config
docker compose up -d
docker compose ps
docker compose logs kafka
docker compose down
```

Stage 1 scope:

- Kafka runs locally for development.
- Kafka UI is available at [http://localhost:8080](http://localhost:8080/).
- Topics are created automatically: `iot_raw_logs`, `iot_processed_logs`, `iot_invalid_logs`, and `iot_pipeline_alerts`.
- This stage prepares the streaming layer for the future Go producer and Python consumer.

## 8. Stage 2 Go producer

Stage 2 adds a simple Go producer that reads `data/samples/sample_iot_logs.csv`, converts each row into JSON, appends `ingestion_timestamp`, and publishes to Kafka topic `iot_raw_logs`.

Run Stage 2 locally:

```bash
docker compose config
docker compose up -d kafka kafka-ui kafka-init
docker compose run --rm go-producer
docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_raw_logs --from-beginning --max-messages 5
docker compose down
```

How to use this stage:

- Start Kafka and topic initialization with `docker compose up -d kafka kafka-ui kafka-init`.
- Run the producer on demand with `docker compose run --rm go-producer`.
- Verify published messages with the Kafka console consumer command above.
- Open Kafka UI at [http://localhost:8080](http://localhost:8080/) to inspect the `iot_raw_logs` topic and message flow.
- Stop the full local stack with `docker compose down`.

## 9. Stage 3 Python consumer

Stage 3 adds a Python consumer that reads JSON messages from `iot_raw_logs`, validates required fields and data types, normalizes valid records, adds `processed_at`, and routes records to:

- `iot_processed_logs` for valid messages
- `iot_invalid_logs` for invalid messages with `raw_payload`, `error_reason`, and `failed_at`

Run Stage 3 locally:

```bash
docker compose config
docker compose up -d kafka kafka-ui kafka-init
docker compose run --build --rm go-producer
docker compose run --build --rm python-consumer
docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_processed_logs --from-beginning --max-messages 5
docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_invalid_logs --from-beginning --max-messages 5
docker compose down
```

How to use this stage:

- Start Kafka and topic initialization with `docker compose up -d kafka kafka-ui kafka-init`.
- Run the Go producer on demand with `docker compose run --build --rm go-producer`.
- Run the Python consumer on demand with `docker compose run --build --rm python-consumer`.
- Inspect valid output in `iot_processed_logs`.
- Inspect invalid output in `iot_invalid_logs`.
- Use Kafka UI at [http://localhost:8080](http://localhost:8080/) to review topic contents.

## 10. Stage 4A PostgreSQL foundation

Stage 4A adds a local PostgreSQL warehouse foundation for future storage and analytics stages. At this point, the repository only provisions the database container and initializes the base tables automatically. No warehouse loader, dbt, Airflow, Spark, AWS, or Terraform execution logic is introduced in this stage.

Run Stage 4A locally:

```bash
docker compose config
docker compose up -d postgres
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "\dt"
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM processed_iot_logs;"
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM invalid_iot_logs;"
docker compose down
```

What this stage provides:

- a local PostgreSQL 16 service for development
- automatic schema initialization from `storage/postgres/init/001_create_tables.sql`
- persistent storage through a named Docker volume
- health checks for service readiness
- base tables `processed_iot_logs` and `invalid_iot_logs`

## 11. Stage 4B warehouse loader

Stage 4B adds a warehouse loader service that consumes records from Kafka topics `iot_processed_logs` and `iot_invalid_logs`, then inserts them into PostgreSQL tables `processed_iot_logs` and `invalid_iot_logs`.

Run Stage 4B tests:

```bash
docker compose config
docker compose run --build --rm warehouse-loader python -m pytest
```

Run Stage 4B end-to-end verification for valid records:

```bash
docker compose config
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage4-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage4-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM processed_iot_logs;"
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT * FROM processed_iot_logs LIMIT 5;"
```

Run Stage 4B end-to-end verification for invalid records:

```bash
'{"event_timestamp": }' | docker exec -i iot-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot_raw_logs
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage4-invalid -e CONSUMER_MAX_MESSAGES=1 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage4-invalid-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=1 warehouse-loader
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT COUNT(*) FROM invalid_iot_logs;"
docker exec -it iot-postgres psql -U iot_user -d iot_logs -c "SELECT * FROM invalid_iot_logs LIMIT 5;"
docker compose down
```

What this stage provides:

- a dedicated `warehouse-loader` Python service
- Kafka consumption from `iot_processed_logs` and `iot_invalid_logs`
- PostgreSQL inserts into `processed_iot_logs` and `invalid_iot_logs`
- mapper tests that run without real Kafka or PostgreSQL
- clean exits with max message and idle timeout controls

## 12. Stage 5A dbt foundation and staging models

Stage 5A adds a local dbt foundation on top of the PostgreSQL warehouse. This stage introduces dbt source definitions, staging models, and baseline data quality tests for processed and invalid IoT logs. No dashboard, Airflow, Spark, AWS, Terraform, or CI/CD execution logic is added in this stage.

Run Stage 5A verification:

```bash
docker compose config
docker compose down -v
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage5-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage5-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt debug
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
```

Check created dbt models in PostgreSQL:

```bash
docker exec -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "\dt"
docker exec -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) FROM stg_processed_iot_logs;"
docker exec -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT * FROM stg_processed_iot_logs LIMIT 5;"
```

What this stage provides:

- a Dockerized dbt runtime configured for the local PostgreSQL warehouse
- dbt source definitions for `processed_iot_logs` and `invalid_iot_logs`
- staging models `stg_processed_iot_logs` and `stg_invalid_iot_logs`
- dbt tests for required fields and allowed categorical values
- documentation for repeatable local verification

## 13. Stage 5B dbt analytics marts

Stage 5B adds analytics marts on top of the Stage 5A staging models. This stage introduces reusable device, attack, protocol, and pipeline quality summaries in dbt. No dashboard, Airflow, Spark, AWS, Terraform, or CI/CD execution logic is added in this stage.

Run Stage 5B verification:

```bash
docker compose config
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
```

What this stage provides:

- analytics marts under `dbt/models/marts/`
- device-level risk scoring with `HIGH`, `MEDIUM`, and `LOW` bands
- attack-type and protocol summaries for downstream analytics
- a one-row pipeline quality mart for processed versus invalid record tracking
- dbt tests for important mart fields

## 14. Stage 6A Streamlit dashboard foundation

Stage 6A adds the first Streamlit dashboard layer for local analytics on top of the existing dbt marts in PostgreSQL. This stage focuses on a safe local foundation only: Dockerized Streamlit, PostgreSQL connectivity through environment variables, basic KPI cards, and simple mart previews. No advanced charts, filters, Airflow, Spark, AWS, Terraform, or CI/CD execution logic is added in this stage.

Run Stage 6A verification:

```bash
docker compose config
docker compose down -v
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage6a-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage6a-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose up --build streamlit-dashboard
```

Open the dashboard at [http://localhost:8501](http://localhost:8501/).

What this stage provides:

- a Dockerized Streamlit service under `dashboard/`
- reusable PostgreSQL connection helpers driven by environment variables
- connection status and friendly error handling in the UI
- basic KPI cards from `mart_pipeline_quality_summary`
- simple table previews for:
  - `mart_device_risk_summary`
  - `mart_attack_summary`
  - `mart_protocol_metrics`
  - `mart_pipeline_quality_summary`

## 15. Stage 6B Streamlit dashboard metrics, charts, and filters

Stage 6B builds on the Stage 6A dashboard foundation and turns it into a practical analytics UI. This stage adds sidebar filters, ranked tables, and simple charts on top of the existing dbt marts in PostgreSQL. The scope remains local dashboard improvements only. No Airflow, Spark, AWS, Terraform, CI/CD, or real credentials are introduced in this stage.

Run Stage 6B verification:

```bash
docker compose config
docker compose down -v
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage6b-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage6b-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose build streamlit-dashboard
docker compose up -d streamlit-dashboard
curl -I http://localhost:8501
```

Open the dashboard at [http://localhost:8501](http://localhost:8501/).

What this stage provides:

- sidebar filters for `risk_level`, `protocol`, `attack_type`, and `top N`
- KPI cards for processed, invalid, total, and invalid-rate metrics
- simple bar charts for device risk, attack volume, and protocol volume
- sorted analytics tables for all current dbt marts
- empty-state messaging when marts are missing or filtered to zero rows

## 16. Stage 6C Streamlit dashboard polish and final documentation

Stage 6C focuses on dashboard polish, UX improvements, and portfolio-ready documentation. The Streamlit layer remains intentionally simple, but now includes clearer guidance in the UI, friendlier empty states, cleaner section descriptions, and better instructions for local verification and screenshot capture. No Airflow, Spark, AWS, Terraform, CI/CD, deployment logic, authentication, or real credentials are introduced in this stage.

Run Stage 6C verification:

```bash
docker compose config
docker compose down -v
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage6c-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage6c-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose build streamlit-dashboard
docker compose up -d --force-recreate streamlit-dashboard
curl -I http://localhost:8501
```

Open the dashboard at [http://localhost:8501](http://localhost:8501/).

What Stage 6C adds:

- clearer section descriptions and explanatory captions in the UI
- more user-friendly warnings when dbt marts are missing or empty
- safer empty-state handling for missing rows and filters returning zero results
- dashboard usage guidance in the sidebar
- final documentation for preparing data, running the dashboard, and capturing screenshots

Dashboard sections:

- `Pipeline Overview` shows processed, invalid, total, and invalid-rate KPIs
- `Device Risk` shows risk distribution, top devices, and ranked device risk rows
- `Attack Summary` shows attack-event volume by attack type
- `Protocol Metrics` shows total events by protocol
- `Raw Mart Tables` shows filtered table previews from the current marts

Portfolio screenshot recommendation:

- store screenshots in `docs/screenshots/`
- useful captures include the dashboard overview, pipeline KPI section, device risk section, and attack/protocol charts

## 17. Stage 7A Airflow orchestration foundation

Stage 7A adds the local Apache Airflow foundation for future orchestration. This stage is intentionally limited to infrastructure and a smoke DAG only. No real producer, consumer, warehouse loader, dbt, dashboard, Spark, AWS, Terraform, CI/CD, deployment, or credential orchestration is introduced here.

Run Stage 7A verification:

```bash
docker compose config
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
docker compose logs airflow-webserver --tail=50
docker compose logs airflow-scheduler --tail=50
```

Open Airflow at [http://localhost:8081](http://localhost:8081/).

Local login:

- username: `airflow`
- password: `airflow`

Local path note:

- `iot_local_pipeline_dag` depends on `HOST_PROJECT_ROOT` so Docker Compose inside Airflow can resolve Windows bind mounts correctly
- use a forward-slash absolute host path such as `C:/Users/User/Desktop/Cloud Technologies/IoT_Log_Intelligence_Pipeline`
- if the repository lives elsewhere on your machine, override `HOST_PROJECT_ROOT` in your local environment or `.env`

What this stage provides:

- a dedicated `airflow-postgres` metadata database separate from the warehouse PostgreSQL service
- `airflow-init`, `airflow-webserver`, and `airflow-scheduler` services in Docker Compose
- mounted local Airflow directories for DAGs, logs, and plugins
- a safe manual smoke DAG named `iot_pipeline_smoke_dag`
- a local Airflow UI on port `8081`

How to validate the smoke DAG:

- start the services with the commands above
- open [http://localhost:8081](http://localhost:8081/)
- sign in with `airflow / airflow`
- confirm that `iot_pipeline_smoke_dag` appears in the DAG list
- optionally trigger the DAG manually to verify that Airflow executes simple tasks successfully

How to stop the Airflow services:

```bash
docker compose stop airflow-webserver airflow-scheduler airflow-postgres
```

## 18. Stage 7B Airflow local pipeline orchestration DAG

Stage 7B builds on the Stage 7A Airflow foundation and adds a manual local orchestration DAG for the existing IoT pipeline steps. This stage remains local-development only. No Spark, AWS, Terraform, CI/CD, deployment logic, authentication, or real credentials are introduced here.

Run Stage 7B verification:

```bash
docker compose config
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags list
```

Expected DAGs:

- `iot_pipeline_smoke_dag`
- `iot_local_pipeline_dag`

Open Airflow at [http://localhost:8081](http://localhost:8081/).

Local login:

- username: `airflow`
- password: `airflow`

How to trigger the local orchestration DAG:

- open [http://localhost:8081](http://localhost:8081/)
- sign in with `airflow / airflow`
- find `iot_local_pipeline_dag`
- trigger it manually from the Airflow UI
- monitor the tasks in graph or grid view

Task order in `iot_local_pipeline_dag`:

- `start`
- `start_infrastructure`
- `run_go_producer`
- `run_python_consumer`
- `run_warehouse_loader`
- `run_dbt_run`
- `run_dbt_test`
- `finish`

What this stage provides:

- a new manual DAG named `iot_local_pipeline_dag`
- orchestration of the existing local Docker Compose pipeline steps through Airflow `BashOperator` tasks
- retained availability of `iot_pipeline_smoke_dag` for lightweight Airflow checks
- minimal local-only Docker support in Airflow via a small custom Airflow image, project-directory mount, and Docker-socket mount

What this DAG does not do:

- it does not start the Streamlit dashboard
- it does not add Spark, AWS, Terraform, CI/CD, deployment, authentication, or real credentials
- it does not rewrite the existing producer, consumer, warehouse-loader, dbt, or dashboard logic

## 19. Stage 7C Airflow safer repeated local runs

Stage 7C improves `iot_local_pipeline_dag` for repeatable local demo runs. This stage keeps the orchestration local-only and avoids destructive resets of Airflow metadata or warehouse volumes. No Spark, AWS, Terraform, CI/CD, deployment logic, authentication, production secrets, or external services are introduced here.

## 20. Stage 7D Airflow documentation and developer-experience polish

Stage 7D focuses on documentation, screenshots guidance, troubleshooting, and PR-readiness polish for the local Airflow orchestration stage. No new cloud, CI/CD, deployment, authentication, or external-service behavior is added here.

Stage 7 summary:

- Airflow runs locally through Docker Compose
- Airflow UI is available at [http://localhost:8081](http://localhost:8081/)
- `iot_pipeline_smoke_dag` verifies basic Airflow functionality
- `iot_local_pipeline_dag` orchestrates the local pipeline for demo runs
- repeated local DAG runs are safer because Kafka runtime state is reset, warehouse pipeline tables are truncated, and consumer group ids are unique per run

High-level `iot_local_pipeline_dag` flow:

- `start`
- `reset_local_pipeline_state`
- `start_infrastructure`
- `truncate_warehouse_tables`
- `run_go_producer`
- `run_python_consumer`
- `run_warehouse_loader`
- `run_dbt_run`
- `run_dbt_test`
- `finish`

Expected successful result:

- Go producer sends `72` records
- Python consumer processes `72` records
- warehouse-loader inserts `72` processed records
- `dbt run` passes
- `dbt test` passes with `53` tests

Important note:

- the Airflow DAG does not start the Streamlit dashboard

Run Stage 7C verification:

```bash
docker compose config
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags show iot_local_pipeline_dag
```

Expected DAGs:

- `iot_pipeline_smoke_dag`
- `iot_local_pipeline_dag`

Updated task order in `iot_local_pipeline_dag`:

- `start`
- `reset_local_pipeline_state`
- `start_infrastructure`
- `truncate_warehouse_tables`
- `run_go_producer`
- `run_python_consumer`
- `run_warehouse_loader`
- `run_dbt_run`
- `run_dbt_test`
- `finish`

What Stage 7C adds:

- safe Kafka reset behavior for local demo reruns without touching Airflow services
- warehouse-table truncation for `processed_iot_logs` and `invalid_iot_logs` with `RESTART IDENTITY`
- unique `CONSUMER_GROUP_ID` and `WAREHOUSE_LOADER_GROUP_ID` values per Airflow `run_id`
- DAG-level markdown documentation, clearer tags, retries, retry delay, and task execution timeouts

What Stage 7C does not do:

- it does not reset the Airflow metadata database
- it does not run `docker compose down -v`
- it does not remove PostgreSQL warehouse volumes
- it does not start the Streamlit dashboard
- it does not add any cloud or production orchestration

## 21. Basic CI with GitHub Actions

GitHub Actions now provides a lightweight CI workflow for pull requests and pushes targeting `develop` and `main`.

The current CI scope is intentionally small and focused on core quality checks:

- validate the Docker Compose configuration with `docker compose config`
- run Go tests in `go-producer`
- run Python consumer tests in `python-consumer`
- run warehouse-loader tests in `warehouse-loader`
- parse and compile the dbt project in `dbt/`
- validate Airflow DAG syntax and imports in `airflow/dags/`

The dbt CI step checks project syntax, model references, and compilation safety without running the full warehouse pipeline.

The Airflow CI step checks DAG syntax and import safety without starting the Airflow webserver or scheduler and without triggering DAG runs.

Full pipeline execution still happens locally through Docker Compose and Airflow. CI does not yet start Kafka, PostgreSQL, Airflow services, Streamlit, or the full dbt runtime for `dbt run` / `dbt test`, and it does not include deployment, registry publishing, cloud infrastructure, or production secrets.

## 22. Stage 9A PySpark batch processing foundation

Stage 9A adds the local PySpark foundation for future batch processing work. This stage introduces a Dockerized local-mode Spark runtime and a smoke job. No existing producer, consumer, warehouse-loader, dbt, Streamlit, or Airflow logic is changed.

Run Stage 9A verification:

```bash
docker compose config
docker compose run --build --rm spark-batch
```

What this stage provides:

- a dedicated `spark/` workspace for local batch jobs
- a minimal `spark-batch` Docker Compose service that runs on demand
- a local `PySpark` runtime pinned in `spark/requirements.txt`
- a smoke job that starts a `SparkSession`, creates a tiny in-memory DataFrame, runs a simple aggregation, prints the result, and exits cleanly

What this stage does not do:

- it does not implement full batch feature engineering
- it does not integrate Spark into Airflow yet
- it does not depend on Kafka, dbt, Streamlit, or PostgreSQL for the smoke test
- it does not add AWS, EMR, Glue, S3, Terraform, Kubernetes, deployment, or secrets management
- it does not create a real Spark cluster; this is local PySpark only

## 23. Stage 9B PySpark device feature engineering

Stage 9B builds on Stage 9A and adds the first real local Spark batch transformation: device-level feature engineering from sample IoT logs into Parquet output. Spark still runs in local mode through Docker and is still separate from Airflow orchestration.

Input and output:

- input path: `data/samples/sample_iot_logs.csv`
- output path: `data/processed/spark/device_features`
- output format: Parquet

Expected sample-data result:

- `72` input rows
- `24` device-level feature rows

Run Stage 9B verification:

```bash
docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py
docker compose run --rm spark-batch ls -la /app/data/processed/spark/device_features
```

What this stage provides:

- a PySpark batch job under `spark/jobs/device_features_job.py`
- local device-level aggregates such as event counts, protocol counts, packet-size metrics, duration metrics, failed and success counts, attack ratios, first and last event timestamps, and `risk_level`
- overwrite-safe local Parquet output for repeated development runs

What this stage does not do:

- it does not integrate Spark into Airflow yet
- it does not write Spark results into PostgreSQL yet
- it does not use S3, EMR, Glue, Terraform, or any cloud infrastructure yet
- it does not add deployment, Kubernetes, or secrets management

## 24. Stage 10 Airflow + PySpark integration

Stage 10 builds on the earlier local Airflow and PySpark foundations and connects them in the existing local orchestration DAG. The PySpark device feature engineering job now runs as part of `iot_local_pipeline_dag`, and a follow-up validation step confirms that Spark produced Parquet output successfully. Spark still runs in local Docker mode only.

Airflow tasks added in this stage:

- `run_spark_device_features`
- `validate_spark_device_features_output`

Current task order around dbt and Spark:

- `run_dbt_run`
- `run_dbt_test`
- `run_spark_device_features`
- `validate_spark_device_features_output`
- `finish`

Spark output details:

- output path: `data/processed/spark/device_features`
- output format: Parquet
- validation checks that the output directory exists
- validation checks that at least one `.parquet` file exists in the output directory

Stage 10 verification commands:

```bash
docker compose config
docker compose run --rm airflow-webserver python -m py_compile /opt/airflow/dags/iot_local_pipeline_dag.py
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
```

How to verify this stage manually:

- trigger `iot_local_pipeline_dag` from the Airflow UI
- confirm that `run_spark_device_features` succeeds after `run_dbt_test`
- confirm that `validate_spark_device_features_output` succeeds before `finish`
- confirm that `data/processed/spark/device_features` contains Parquet files after the DAG run

What this stage adds:

- local Airflow orchestration of the PySpark device feature engineering job
- an Airflow validation step that checks for generated Parquet output
- clearer local verification commands for the updated DAG

What this stage does not add:

- AWS, EMR, Glue, S3, Terraform, or Kubernetes
- Spark output quality checks beyond file existence
- PostgreSQL loading for Spark output
- production deployment or secret-management changes

## 25. Stage 11A local S3-compatible data lake foundation

Stage 11A adds a local S3-compatible object storage foundation through MinIO. This stage is intentionally limited to local Docker Compose infrastructure and documentation only. It does not upload Spark outputs, it does not integrate MinIO into Airflow, and it does not introduce real AWS S3, EMR, Glue, Terraform, Kubernetes, deployment, or secrets.

MinIO local endpoints:

- S3 API: [http://localhost:9000](http://localhost:9000/)
- MinIO console: [http://localhost:9001](http://localhost:9001/)

Local credentials:

- username: `minioadmin`
- password: `minioadmin`

Bucket created in this stage:

- `iot-data-lake`

Run Stage 11A verification:

```bash
docker compose config
docker compose --profile object-storage up -d minio minio-init
docker compose ps
docker compose --profile object-storage run --rm minio-init
```

How to use this stage:

- start MinIO with `docker compose --profile object-storage up -d minio minio-init`
- open the MinIO console at [http://localhost:9001](http://localhost:9001/) and sign in with `minioadmin / minioadmin`
- verify the `iot-data-lake` bucket exists either in the console or by re-running `docker compose --profile object-storage run --rm minio-init`
- review the focused setup guide in [docs/object-storage.md](docs/object-storage.md)

What this stage provides:

- a local `minio` service for S3-compatible storage development
- an idempotent `minio-init` service that creates `iot-data-lake`
- a named Docker volume for MinIO data persistence
- environment placeholders for local object storage configuration

What this stage does not do:

- it does not upload Spark outputs yet
- it does not integrate MinIO into Airflow yet
- it does not add AWS S3, EMR, Glue, Terraform, Kubernetes, deployment, or secrets

## 26. Security note

Do not commit real credentials, production secrets, or sensitive data. Use environment variables and secret management outside the repository.

## 27. Current stage

Stage 11A includes:

- repository skeleton and documentation
- local Docker Compose services for Kafka, Kafka topic initialization, and Kafka UI
- a Go producer that reads sample CSV data and publishes JSON messages to Kafka
- a Python consumer that validates and routes records to processed and invalid Kafka topics
- a local PostgreSQL foundation with automatic table initialization for processed and invalid IoT logs
- a warehouse loader that consumes processed and invalid Kafka topics and writes to PostgreSQL
- a dbt project with PostgreSQL sources, staging tables, and baseline tests
- analytics marts for device risk, attack summary, protocol metrics, and pipeline quality
- a polished Streamlit dashboard with KPI cards, filters, charts, mart tables, and portfolio-ready UX guidance
- a local Apache Airflow foundation with a separate metadata database, webserver, scheduler, smoke DAG, safer repeatable local orchestration DAG, and polished local documentation
- a lightweight GitHub Actions CI workflow for Docker Compose validation, Go and Python tests, safe dbt parse/compile checks, and Airflow DAG syntax/import validation on `develop` and `main`
- a local PySpark batch-processing foundation with a dedicated `spark-batch` Docker Compose service and a simple smoke job
- a local PySpark device feature engineering job that reads `data/samples/sample_iot_logs.csv` and writes Parquet output to `data/processed/spark/device_features`
- integration of that PySpark device feature job into `iot_local_pipeline_dag` through `run_spark_device_features`
- Airflow-side Spark output validation through `validate_spark_device_features_output`
- a local MinIO object storage foundation with S3-compatible API access on port `9000`, a MinIO console on port `9001`, and automatic creation of bucket `iot-data-lake`
- safe local environment placeholders

Airflow now orchestrates the existing local producer, consumer, warehouse loader, dbt flow, PySpark device feature engineering step, and a simple Spark output existence check through one manual DAG that is safer for repeated demo runs and better documented for local development. Spark still runs only in local Docker mode and the validation step checks output existence only; it does not yet perform deeper data-quality validation or write Spark results into PostgreSQL. MinIO now provides a local S3-compatible data lake foundation only; Spark output upload and Airflow integration are intentionally deferred. The project still does not add production AWS S3, EMR, Glue, Terraform, Kubernetes, deployment tooling, or production secrets. Full dbt execution and full Airflow orchestration are still verified locally through Docker Compose or Airflow, while CI remains limited to safe validation checks.
