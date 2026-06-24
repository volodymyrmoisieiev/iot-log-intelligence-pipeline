# IoT Log Intelligence Pipeline

[![CI](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/ci.yml)
[![Terraform Validate](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/terraform-validate.yml/badge.svg)](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/terraform-validate.yml)
[![Python and Airflow Validate](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/python-airflow-validate.yml/badge.svg)](https://github.com/volodymyrmoisieiev/iot-log-intelligence-pipeline/actions/workflows/python-airflow-validate.yml)

## CI Quality Gates

- `CI` covers repository structure, Docker Compose validation, and lightweight Python syntax checks.
- `Terraform Validate` covers `infra/aws-orchestration/` formatting and config validation without backend state or AWS credentials.
- `Python and Airflow Validate` covers Python entry-point compilation and Airflow DAG syntax validation.
- The final Stage 20 runbook lives at [docs/stage-20-ci-quality-gates.md](docs/stage-20-ci-quality-gates.md).

## 1. Project overview

IoT Log Intelligence Pipeline is a portfolio project focused on end-to-end data engineering for IoT logs: ingestion, processing, storage, transformation, and analytics.

The repository is currently at Stage 21A, with a working local Kafka stack, a Go producer, a Python consumer validation layer, a local PostgreSQL warehouse foundation, a warehouse loader service, dbt staging models, dbt analytics marts on top of PostgreSQL, an optional Snowflake-ready dbt target for future cloud warehouse integration, a polished Streamlit dashboard for local analytics, a Streamlit observability monitoring section, safer repeatable local Apache Airflow orchestration for the existing pipeline steps, a fast Stage 20A GitHub Actions CI foundation for repository structure, Docker Compose validation, and lightweight Python syntax checks on pull requests and pushes to `develop` and `main`, a dedicated Stage 20B Terraform validation GitHub Actions workflow for the AWS orchestration foundation under `infra/aws-orchestration/`, a dedicated Stage 20C Python and Airflow validation GitHub Actions workflow for lightweight script compilation and DAG syntax checks, Stage 20D pull-request and release-process guidance with visible README CI quality badges, a final Stage 20E CI quality-gates runbook with PR-ready cleanup guidance, and a Stage 21A local E2E smoke-test foundation that reuses those safe checks through one bounded JSON-reporting entry point without starting the full local pipeline by default, alongside a local PySpark batch-processing foundation with a device-level feature engineering job that runs inside the local Airflow pipeline, a local MinIO-based S3-compatible object storage foundation, a local uploader that sends Spark device-feature Parquet output into MinIO, Airflow integration that uploads and validates those MinIO objects as part of the local DAG, an AWS-ready Terraform foundation, Terraform S3 data lake definitions for future AWS storage, an AWS cloud orchestration Terraform foundation for future Lambda, Step Functions, CloudWatch, IAM, and S3-integrated control-plane work, a local AWS Lambda metadata-validation foundation for cloud-side file intake, a Step Functions orchestration foundation for validation-first AWS workflow design, a CloudWatch monitoring and alarms foundation for orchestration observability, and a final Stage 19 AWS orchestration runbook for PR-ready cloud-foundation documentation, alongside the existing observability schema foundation for pipeline audit history, quality checks, and alerts, a local observability writer that persists warehouse-derived metrics into those audit tables, can optionally publish alert events to Kafka, runs near the end of the local Airflow DAG, dataset profile support that now extends through producer, consumer, loader, and the local Airflow runbook for `sample`, `medium`, and `full` style validation runs, a local performance benchmark foundation for benchmark execution, Markdown summary generation, and bottleneck-focused analysis, a Stage 17 data contract foundation, local CSV validation tooling, an Airflow pre-check for the raw IoT log schema, and a Stage 18 anomaly detection foundation with a standalone script, warehouse persistence, Airflow integration, and final PR-ready documentation.

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
|-- contracts/
|-- dbt/models/
|   |-- staging/
|   |-- intermediate/
|   `-- marts/
|-- docs/
|   |-- aws-cloud-orchestration.md
|   |-- stage-19-aws-cloud-orchestration.md
|   `-- screenshots/
|-- go-producer/
|-- infra/
|   |-- aws-orchestration/
|   `-- terraform/
|       |-- aws/
|       |-- modules/
|       |   |-- cloudwatch/
|       |   |-- iam/
|       |   |-- lambda/
|       |   |-- s3/
|       |   `-- step_functions/
|       `-- environments/
|           |-- dev/
|           `-- prod/
|-- object-storage/
|-- observability/
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
- Stage 11B: Spark device features upload to local MinIO
- Stage 11C: Airflow + MinIO integration
- Stage 12A: Terraform AWS infrastructure foundation
- Stage 12B: AWS resources and data lake infrastructure
- Stage 12C: Terraform validation in CI
- Stage 13: Snowflake-ready warehouse option
- Stage 14: pipeline observability and data quality alerts
- Stage 15: dataset processing modes
- Stage 16: performance / load testing
- Stage 17: data contracts + stronger validation
- Stage 18: anomaly detection / suspicious traffic detection
- Stage 19: AWS cloud orchestration foundation
- Stage 20A: CI foundation quality gates
- Stage 20B: dedicated Terraform validation workflow
- Stage 20C: Python and Airflow validation workflow
- Stage 20D: PR template, release checklist, and README CI badges
- Stage 20E: final CI quality-gates runbook and PR-ready cleanup
- Stage 21A: local E2E smoke test foundation

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

## 21. Stage 20A-20E CI Quality Gates with GitHub Actions

GitHub Actions now provides separate Stage 20 quality gates for pull requests targeting `develop` and `main`, plus direct pushes to those branches.

Stage 20A remains the fast repository-level foundation workflow in `.github/workflows/ci.yml`. Its scope is intentionally small and focused on safe early validation:

- check out the repository
- print basic repository metadata for debugging
- verify expected project structure and important directories
- validate the Docker Compose configuration with `docker compose config`
- compile selected lightweight Python entry points with `py_compile` when those files exist

Stage 20B adds a dedicated Terraform workflow in `.github/workflows/terraform-validate.yml` for `infra/aws-orchestration/`. That workflow:

- checks Terraform formatting with `terraform fmt -check`
- initializes Terraform with `terraform init -backend=false`
- validates configuration with `terraform validate`

`terraform init -backend=false` keeps the workflow safe for CI because it avoids backend connectivity, remote state access, AWS credentials, and any deployment behavior while still letting Terraform validate configuration structure.

Stage 20C adds a dedicated Python and Airflow workflow in `.github/workflows/python-airflow-validate.yml`. That workflow:

- compiles important Python utility and validation entry points with `py_compile`
- compiles Python DAG files under `airflow/dags/`
- lists discovered DAG Python files for easier troubleshooting

To stay lightweight and reliable, Stage 20C does not start the full Airflow runtime in GitHub Actions, does not run Kafka/PostgreSQL/Spark services, and does not trigger any DAG execution. Full Airflow CLI and runtime verification remain local/manual through Docker Compose when needed.

Together, Stage 20A, Stage 20B, and Stage 20C catch repository-level regressions, Terraform configuration issues, Python syntax regressions, and Airflow DAG syntax problems without starting the full local platform or creating cloud resources.

Stage 20D adds a PR template, a release checklist, and visible README badges for the current GitHub Actions quality gates so contributors and reviewers have a clearer merge and release workflow.

The PR template now lives at `.github/pull_request_template.md` and reinforces validation, risk review, documentation checks, and secret-safety before merge.

The release checklist now lives at `docs/release-checklist.md` and documents the expected branch flow:

- `feature/* -> develop` through pull requests with `Squash and merge`
- `develop -> main` through a release pull request with `Merge pull request`
- local branch cleanup and verification after merge

Stage 20E adds the final Stage 20 runbook at [docs/stage-20-ci-quality-gates.md](docs/stage-20-ci-quality-gates.md), ties together the README, CI guide, and release checklist, and finishes the PR-ready cleanup/documentation pass for this branch.

For the focused Stage 20 guide and expansion roadmap, see [docs/ci-quality-gates.md](docs/ci-quality-gates.md).

## 21. Stage 21B Local E2E smoke test foundation and controlled sample runtime

Stage 21B keeps the safe local smoke-test entry point at `scripts/run_local_e2e_smoke_test.py`, extends it with an optional `--run-sample-pipeline` mode, and updates the focused runbook at [docs/local-e2e-smoke-test.md](docs/local-e2e-smoke-test.md).

What this stage provides:

- a standard-library-only smoke-test helper with `sample`, `medium`, and `full` profile selection
- bounded row inspection through `--max-rows` so repository validation does not scan the full dataset by default
- JSON result reporting for repository structure, dataset selection, Docker Compose config, Python syntax, Terraform validation, data-contract validation, and optional read-only anomaly detection
- an optional controlled sample runtime E2E pass that starts required local services, runs producer/consumer/warehouse-loader with matching bounded limits, and verifies PostgreSQL row-count deltas
- explicit safety rails that avoid starting the full Kafka/PostgreSQL pipeline, avoid AWS deployment, and avoid `terraform apply`

Recommended sample-safe commands:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --dry-run --output-json docs/e2e-smoke-test-local.json
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --output-json docs/e2e-smoke-test-local.json
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-sample-pipeline --output-json docs/e2e-smoke-test-local.json
```

Stage 21B still keeps the default flow sample-safe and bounded. Full dataset or `100k` style validation remains future Stage 21C / Stage 21D work rather than the default local behavior here.

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

## 26. Stage 11B Spark device features upload to local MinIO

Stage 11B builds on the Stage 11A MinIO foundation and adds a local uploader for Spark device-feature Parquet files. This stage remains local-only and still avoids Airflow integration, production AWS S3, EMR, Glue, Terraform, Kubernetes, deployment, and secrets.

Local Spark input path:

- `data/processed/spark/device_features`

MinIO upload target:

- bucket: `iot-data-lake`
- object prefix: `spark/device_features/latest/`

Upload script and service:

- script: `object-storage/upload_spark_features.py`
- Docker Compose service: `object-storage-uploader`

Run Stage 11B verification:

```bash
docker compose config
docker compose --profile object-storage up -d minio minio-init
docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py
docker compose --profile object-storage run --build --rm object-storage-uploader
docker compose logs minio-init --tail=20
```

How to use this stage:

- make sure MinIO is running with `docker compose --profile object-storage up -d minio minio-init`
- generate fresh Spark Parquet output with `docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py`
- upload the Parquet files with `docker compose --profile object-storage run --build --rm object-storage-uploader`
- verify uploaded objects in the MinIO console at [http://localhost:9001](http://localhost:9001/) or through uploader logs

What this stage provides:

- a local `boto3`-based upload script for Spark Parquet artifacts
- clear failures when the local Spark output folder is missing or empty
- uploads into `iot-data-lake` under `spark/device_features/latest/`
- a dedicated Docker Compose service under the `object-storage` profile

What this stage does not do:

- it does not integrate the upload into Airflow yet
- it does not upload to real AWS S3
- it does not add EMR, Glue, Terraform, Kubernetes, deployment, or secrets

## 27. Stage 11C Airflow + MinIO integration

Stage 11C builds on the Stage 11B uploader and wires it into the existing local Airflow orchestration DAG. After Spark device features are generated and validated locally, Airflow now starts local MinIO services, uploads the Parquet files to MinIO, and validates that uploaded `.parquet` objects exist under the expected prefix. This remains local MinIO only, not production AWS S3.

Airflow MinIO target:

- bucket: `iot-data-lake`
- object prefix: `spark/device_features/latest/`

New Airflow tasks:

- `start_object_storage`
- `upload_spark_features_to_minio`
- `validate_minio_spark_features_upload`

Updated task order after dbt:

- `run_dbt_test`
- `run_spark_device_features`
- `validate_spark_device_features_output`
- `start_object_storage`
- `upload_spark_features_to_minio`
- `validate_minio_spark_features_upload`
- `finish`

Run Stage 11C verification:

```bash
docker compose config
docker compose run --rm airflow-webserver python -m py_compile /opt/airflow/dags/iot_local_pipeline_dag.py
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
docker compose run --rm airflow-webserver bash -lc "cd /opt/project && docker compose -p iot_log_intelligence_pipeline -f /opt/project/docker-compose.yml --profile object-storage run --rm --entrypoint /bin/sh minio-init -ec 'mc alias set local \"$MINIO_ENDPOINT\" \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" && mc ls --recursive local/$MINIO_BUCKET/spark/device_features/latest/'"
```

What this stage provides:

- local Airflow orchestration for the existing MinIO uploader
- Airflow-side validation that uploaded MinIO objects exist
- continued use of local bucket `iot-data-lake` and prefix `spark/device_features/latest/`

What this stage does not do:

- it does not add production AWS S3
- it does not add EMR, Glue, Terraform, Kubernetes, deployment, or secrets
- it does not change the Spark job logic itself

## 28. Stage 12C Terraform validation in GitHub Actions and documentation polish

Stage 12C builds on the Terraform AWS foundation and S3 data lake definitions under `infra/terraform/aws/` and originally added Terraform validation to GitHub Actions. The current repository keeps that Terraform foundation in place, while the Stage 20A CI workflow is intentionally narrowed back to faster repository-level checks before deeper infrastructure validation returns in a later Stage 20 phase.

What this stage provides:

- a dedicated Terraform root module for future AWS infrastructure
- variable-driven AWS provider configuration
- shared inputs for `aws_region`, `project_name`, `environment`, and `data_lake_bucket_name`
- an AWS S3 data lake bucket definition with versioning, server-side encryption, public access blocking, and ownership controls
- documented logical prefixes for future lake paths such as `raw/`, `processed/`, and `spark/device_features/latest/`
- a documented Terraform validation path using `terraform fmt -check -recursive`, `terraform init -backend=false`, and `terraform validate`
- safe documentation for local `terraform fmt`, `terraform init`, `terraform validate`, and credential-aware `terraform plan`

What this stage does not do:

- it does not create any AWS resources until `terraform apply` is run
- it does not upload any real data objects into S3 yet
- it does not run `terraform apply` in this stage
- it does not run `terraform plan` in CI
- it does not modify the local application services or pipeline behavior

AWS credentials are not required for CI validation because Terraform initialization runs with `-backend=false` and validation checks syntax and provider configuration only. AWS credentials are required only for real Terraform plan or apply workflows against AWS. The S3 bucket is intended for future Spark and Parquet outputs after later stages extend the cloud data lake flow.

## 29. Stage 13A Snowflake dbt target foundation

Stage 13A keeps PostgreSQL as the default local dbt warehouse target and adds an optional Snowflake-ready profile target for future cloud warehouse integration. The Snowflake configuration is environment-variable-driven only, includes placeholder values only, and does not introduce any real credentials, mandatory cloud dependencies, or CI requirements for a live Snowflake connection.

What this stage provides:

- the existing local PostgreSQL dbt workflow remains the default path
- an optional `snowflake` dbt target under `dbt/profiles.yml`
- Snowflake placeholder environment variables in `.env.example`
- an optional `dbt/requirements-snowflake.txt` dependency file for the Snowflake adapter
- documentation for `dbt debug`, `dbt run`, and `dbt test` with the Snowflake target

Recommended local PostgreSQL dbt validation stays Docker-based:

```powershell
docker compose run --build --rm dbt dbt debug
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
```

Local Windows dbt note:

- if you run dbt directly from a local Python environment, PostgreSQL commands need `dbt-postgres`
- Snowflake commands need `dbt-snowflake`
- if you want both targets locally in one Python environment, install both adapters

Safe repository-root Snowflake syntax/config validation with placeholder values:

```powershell
dbt parse --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

Snowflake commands below are only for real credentials:

```powershell
dbt debug --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt run --project-dir .\dbt --profiles-dir .\dbt --target snowflake
dbt test --project-dir .\dbt --profiles-dir .\dbt --target snowflake
```

Placeholder values such as `your_snowflake_account` are examples only. `dbt parse` is the safe local validation step for that placeholder setup, while real Snowflake connection checks through `dbt debug`, `dbt run`, and `dbt test` are expected to fail until valid credentials and account-specific values are provided through environment variables.

For the full Snowflake dbt runbook, troubleshooting notes, and safety guidance, see [docs/snowflake.md](docs/snowflake.md).

What this stage does not do:

- it does not remove or replace the PostgreSQL target
- it does not commit real Snowflake credentials or account identifiers
- it does not require Snowflake connectivity in CI
- it does not run Terraform apply or any cloud deployment command

## 30. Stage 14 observability foundation

Stage 14 adds local pipeline observability in five steps:

- Stage 14A introduces additive PostgreSQL tables for pipeline run history, quality-check results, and alerts.
- Stage 14B adds a local Python writer that reads existing warehouse row counts, calculates `invalid_rate`, and persists audit rows, quality checks, and alerts for one `run_id`.
- Stage 14C adds optional Kafka publishing for generated observability alerts on topic `iot_pipeline_alerts`.
- Stage 14D adds Docker Compose and Airflow DAG integration for the observability writer and output validation.
- Stage 14E adds a Streamlit `Pipeline Monitoring` section for observability data.

What this stage provides:

- automatic first-run PostgreSQL initialization from `storage/postgres/init/002_create_observability_tables.sql`
- additive tables `pipeline_run_audit`, `pipeline_quality_checks`, and `pipeline_alerts`
- indexes for common observability lookups such as `run_id`, statuses, severities, alert levels, and timestamps
- a local writer script at `observability/write_pipeline_observability.py`
- optional Kafka publishing for generated alert rows through `--publish-alerts`
- Airflow DAG tasks `run_observability_writer` and `validate_observability_output` near the end of the local orchestration flow
- a dashboard `Pipeline Monitoring` section backed by PostgreSQL observability tables
- a focused runbook at [docs/observability.md](docs/observability.md) for manual application against an existing local PostgreSQL volume and local writer validation without deleting data

What this stage does not do:

- it does not add a quality monitor service yet
- it does not change Airflow DAG logic yet
- it does not change dashboard behavior yet

## 31. Stage 15A dataset profile foundation

Stage 15A introduces the repository foundation for future dataset processing modes without changing the current pipeline runtime behavior yet.

What this stage provides:

- a shared dataset profile config at `data/dataset_profiles.yml`
- a documented `sample` dataset path at `data/samples/sample_iot_logs.csv`
- a documented `medium` dataset path at `data/processed/medium_iot_logs.csv`
- a documented `full` dataset path at `data/raw/full_iot_logs.csv`
- documentation for when `sample`, `medium`, and `full` should be used
- git-ignore guidance so large raw or generated datasets stay out of normal repository history

What this stage does not do:

- it does not change the Go producer input logic yet
- it does not change the Python consumer logic yet
- it does not change the warehouse-loader logic yet
- it does not change Airflow DAG logic yet
- it does not change dbt models yet

For the focused runbook, profile definitions, and validation commands, see [docs/dataset-profiles.md](docs/dataset-profiles.md).

## 32. Stage 15B dataset preparation script

Stage 15B adds a local helper script at `scripts/create_dataset_profile.py` that can generate a `medium` CSV from a larger IoT CSV input while preserving the Stage 15A git-safety rules for generated datasets.

## 33. Stage 15C producer dataset profile support

Stage 15C adds dataset profile support to the Go producer while keeping the sample dataset as the default behavior.

What this stage provides:

- `DATASET_PROFILE` support with `sample`, `medium`, and `full`
- `PRODUCER_INPUT_FILE` override support preserved
- optional `PRODUCER_MAX_ROWS` limiting for smaller validation runs
- clearer startup logging and better missing-file guidance for generated `medium` inputs

For the focused producer examples and profile runbook, see [docs/dataset-profiles.md](docs/dataset-profiles.md) and [go-producer/README.md](go-producer/README.md).

## 34. Stage 15D consumer and warehouse loader safety

Stage 15D adds progress-interval controls, clearer idle-timeout exits, and end-of-run summaries for the Python consumer and warehouse loader so medium and full dataset validation runs are easier to monitor safely.

## 35. Stage 15E Airflow dataset mode integration

Stage 15E wires dataset-mode settings into the local Airflow orchestration DAG and finalizes the Stage 15 runbook for sample-safe defaults plus intentional medium-profile overrides.

## 36. Stage 16A Performance / Load Testing

Stage 16A starts the repository's performance and load-testing work with local benchmark tooling for the existing `sample`, `medium`, and `full` dataset profiles.

Stage 16B extends that foundation with optional Markdown benchmark summary generation for human-readable portfolio documentation.

Stage 16C adds local benchmark analysis tooling that aggregates JSON results into bottleneck-focused summaries.

Stage 16D finalizes the documentation and workflow guidance for benchmark execution, summary generation, and result analysis.

What this stage provides:

- a lightweight local benchmark helper at `scripts/run_performance_benchmark.py`
- Docker Compose-based benchmark execution for the Go producer, Python consumer, and warehouse loader
- JSON result capture under `docs/performance/results/`
- a focused runbook at [docs/performance/README.md](docs/performance/README.md)

What this stage does not do:

- it does not change producer runtime logic
- it does not change consumer runtime logic
- it does not change warehouse-loader runtime logic
- it does not change Airflow DAG logic
- it does not change dbt, Spark, MinIO, or Terraform behavior

## 37. Stage 17 Data Contracts + Stronger Validation

Stage 17A adds the first repository data contract for the raw IoT log CSV schema. Stage 17B adds local contract validation tooling through `scripts/validate_data_contract.py`. Stage 17C adds an Airflow pre-check that validates raw input data before producer execution. These stages do not change producer runtime logic, consumer runtime logic, warehouse-loader runtime logic, dbt models, Spark jobs, MinIO logic, Terraform logic, or benchmark scripts.

What this stage provides:

- a raw IoT log contract at [contracts/iot_raw_log_contract.yml](contracts/iot_raw_log_contract.yml)
- a focused data contract guide at [docs/data-contracts.md](docs/data-contracts.md)
- a Stage 17 runbook at [docs/stage-17-data-contracts-validation.md](docs/stage-17-data-contracts-validation.md)
- a local CSV validator at `scripts/validate_data_contract.py`
- an Airflow pre-check task that fails early on raw-data contract violations

## 38. Stage 18 Anomaly Detection / Suspicious Traffic Detection

Stage 18 starts the repository's anomaly-detection work for processed IoT logs. Stage 18A adds a local, standalone rule-based detection foundation that scans recent `processed_iot_logs` rows, applies explainable suspicious-traffic heuristics, prints a clear summary, and can write a local JSON report without changing the existing runtime pipeline behavior. Stage 18B extends that foundation by persisting detected anomalies into warehouse table `iot_anomalies`. Stage 18C integrates anomaly detection into the local Airflow pipeline after warehouse loading. Stage 18D finalizes the documentation and PR-ready validation flow.

What this stage provides:

- a local anomaly detection job at `scripts/run_anomaly_detection.py`
- additive PostgreSQL anomaly table SQL at `storage/postgres/init/04_create_iot_anomalies.sql`
- rule-based checks for packet size, duration, status, attack labels, and unusual protocols
- a focused runbook at [docs/anomaly-detection.md](docs/anomaly-detection.md)
- a final Stage 18 runbook at [docs/stage-18-anomaly-detection.md](docs/stage-18-anomaly-detection.md)
- local JSON report output support for manual review
- optional persistence of anomaly records into the warehouse with `run_id` tracking
- Airflow orchestration of anomaly detection after warehouse loading with `ANOMALY_DETECTION_LIMIT` support

What this stage does not do:

- it does not change Go producer runtime logic
- it does not change Python consumer runtime logic
- it does not change warehouse-loader runtime logic
- it does not change dbt, Spark, MinIO, Terraform, benchmark, or data contract behavior

## 39. Stage 19 AWS Cloud Orchestration Foundation

Stage 19E finalizes the cloud-ready orchestration, Lambda validation, Step Functions control-plane, and CloudWatch monitoring foundation on top of the existing AWS S3 Terraform work without migrating the local runtime pipeline to AWS yet.

What this stage provides:

- a new Terraform root at `infra/aws-orchestration/` for future Lambda, Step Functions, CloudWatch, IAM, and S3-linked orchestration work
- shared naming, tagging, provider, and variable foundations for cloud-side orchestration resources
- a local metadata-validator Lambda foundation under `aws/lambda/iot_metadata_validator/`
- a Step Functions orchestration foundation that validates metadata before future processing
- a CloudWatch monitoring and alarms foundation for the AWS orchestration layer
- a final Stage 19 runbook at [docs/stage-19-aws-cloud-orchestration.md](docs/stage-19-aws-cloud-orchestration.md)
- cost-safe placeholder structure with creation toggles disabled by default
- focused architecture documentation at [docs/aws-cloud-orchestration.md](docs/aws-cloud-orchestration.md)

What this stage does not do:

- it does not change the local pipeline runtime logic
- it does not change Docker Compose behavior
- it does not migrate Airflow, dbt, Spark, MinIO, anomaly detection, or data contracts to AWS yet

## 40. Security note

Do not commit real credentials, production secrets, or sensitive data. Use environment variables and secret management outside the repository.

## 41. Current stage

Stage 21B includes everything from Stage 21A plus:

- a local smoke-test helper at `scripts/run_local_e2e_smoke_test.py`
- a focused local smoke-test runbook at `docs/local-e2e-smoke-test.md`
- bounded JSON-reporting validation for repository structure, Docker Compose config, Python syntax, Terraform validation, data contracts, and optional read-only anomaly detection
- an optional controlled sample runtime E2E mode with isolated Kafka topics, bounded producer/consumer/loader limits, and PostgreSQL row-count verification

Stage 21A, Stage 20E, Stage 20D, Stage 20C, Stage 20B, Stage 20A, Stage 19E, Stage 19D, Stage 19C, Stage 19B, Stage 19A, and Stage 18D foundations remain in place, including:

- repository skeleton and documentation
- local Docker Compose services for Kafka, Kafka topic initialization, and Kafka UI
- a Go producer that reads sample CSV data and publishes JSON messages to Kafka
- a Python consumer that validates and routes records to processed and invalid Kafka topics
- a local PostgreSQL foundation with automatic table initialization for processed and invalid IoT logs
- a warehouse loader that consumes processed and invalid Kafka topics and writes to PostgreSQL
- a dbt project with PostgreSQL sources, staging tables, baseline tests, and an optional Snowflake-ready target template
- analytics marts for device risk, attack summary, protocol metrics, and pipeline quality
- a polished Streamlit dashboard with KPI cards, filters, charts, mart tables, and portfolio-ready UX guidance
- a local Apache Airflow foundation with a separate metadata database, webserver, scheduler, smoke DAG, safer repeatable local orchestration DAG, and polished local documentation
- a fast GitHub Actions CI foundation for repository structure validation, Docker Compose validation, and lightweight Python syntax checks on `develop` and `main`
- a dedicated GitHub Actions Terraform validation workflow for `infra/aws-orchestration/` that runs without AWS credentials or backend state access
- a dedicated GitHub Actions Python and Airflow validation workflow for lightweight script compilation and DAG syntax checks
- a PR template, release checklist, and README CI badges that make the quality-gate process more explicit and portfolio-ready
- a final CI quality-gates runbook that explains local equivalents, CI boundaries, and future follow-up ideas
- a local PySpark batch-processing foundation with a dedicated `spark-batch` Docker Compose service and a simple smoke job
- a local PySpark device feature engineering job that reads `data/samples/sample_iot_logs.csv` and writes Parquet output to `data/processed/spark/device_features`
- integration of that PySpark device feature job into `iot_local_pipeline_dag` through `run_spark_device_features`
- Airflow-side Spark output validation through `validate_spark_device_features_output`
- a local MinIO object storage foundation with S3-compatible API access on port `9000`, a MinIO console on port `9001`, and automatic creation of bucket `iot-data-lake`
- a local object-storage uploader that sends Spark Parquet files from `data/processed/spark/device_features` into MinIO bucket `iot-data-lake` under prefix `spark/device_features/latest/`
- Airflow tasks that start local object storage, upload Spark Parquet files to MinIO, and validate uploaded `.parquet` objects in the bucket
- an AWS-ready Terraform foundation under `infra/terraform/aws/` with provider configuration, shared variables, baseline tags, and safe example inputs
- Terraform definitions for an AWS S3 data lake bucket with versioning, server-side encryption, ownership controls, and public access blocking
- GitHub Actions validation for Terraform formatting, backend-free initialization, and config validation without AWS credentials
- safe local environment placeholders
- optional Snowflake environment placeholders for future cloud warehouse work
- additive PostgreSQL observability tables for pipeline run audits, quality checks, and alerts
- a local Python observability writer that reads warehouse counts and writes observability metrics for one run id
- optional Kafka publishing of generated observability alerts to topic `iot_pipeline_alerts`
- an Airflow-integrated observability writer step and PostgreSQL validation step in `iot_local_pipeline_dag`
- a Streamlit `Pipeline Monitoring` section for latest runs, quality checks, and recent alerts
- a dataset profile foundation that keeps `data/samples/sample_iot_logs.csv` as the default tracked sample dataset and documents planned `medium` and `full` dataset paths for future stages
- a local dataset preparation script that can generate `data/processed/medium_iot_logs.csv` from a larger IoT CSV input for future integration-style validation
- Go producer support for `DATASET_PROFILE`, `PRODUCER_INPUT_FILE` override precedence, and optional `PRODUCER_MAX_ROWS` limiting
- safer Python consumer and warehouse-loader runtime controls for larger dataset validation runs, including progress logging and clearer summaries
- Airflow DAG support for dataset-mode environment settings with sample-safe defaults and documented medium-profile overrides
- a local benchmark helper that times producer, consumer, and warehouse-loader runs for the `sample`, `medium`, and `full` dataset profiles
- a dedicated Stage 16 performance runbook and local JSON benchmark-result output path under `docs/performance/results/`
- optional Markdown benchmark summary generation for local human-readable performance reporting
- a local benchmark analysis helper that aggregates result JSON files into bottleneck-focused summaries
- final Stage 16 documentation that ties benchmark execution, reporting, and analysis into one PR-ready workflow
- a raw IoT log data contract at `contracts/iot_raw_log_contract.yml`
- a Stage 17 data contract guide at `docs/data-contracts.md`
- a local raw-data contract validator at `scripts/validate_data_contract.py`
- an Airflow `validate_raw_data_contract` task before `run_go_producer`
- a local anomaly detection helper at `scripts/run_anomaly_detection.py`
- additive anomaly table SQL at `storage/postgres/init/04_create_iot_anomalies.sql`
- an Airflow `run_anomaly_detection` task after `run_warehouse_loader`
- a Stage 18 anomaly detection guide at `docs/anomaly-detection.md`
- a final Stage 18 runbook at `docs/stage-18-anomaly-detection.md`
- a local E2E smoke-test helper that ties together safe repository-level checks without running the full dataset or full local pipeline by default
- an optional controlled sample runtime flow that exercises the producer, consumer, and warehouse loader with visible bounded limits

Airflow now orchestrates the existing local producer, consumer, warehouse loader, anomaly detection step, dbt flow, PySpark device feature engineering step, local Spark output validation, local MinIO upload, and MinIO object validation through one manual DAG that is safer for repeated demo runs and better documented for local development. Spark still runs only in local Docker mode, and MinIO remains a local S3-compatible target only rather than production AWS S3. Stage 12C keeps the Terraform S3 data lake definitions that mirror the local MinIO pattern for future AWS use and adds CI validation for them, but no AWS resources are created until `terraform apply` is run, and neither `terraform plan` nor `terraform apply` is part of CI. Full dbt execution and full Airflow orchestration are still verified locally through Docker Compose or Airflow, while CI remains limited to safe validation checks. Stage 15A adds the dataset profile contract, Stage 15B adds the local preparation script, Stage 15C brings those profiles into the Go producer, Stage 15D makes larger consumer and loader validation runs clearer and safer, Stage 15E carries those settings into the local Airflow DAG and runbook without changing downstream modeling logic, Stage 16A adds the first local benchmark helper for measuring those profile-specific runs, Stage 16B adds human-readable benchmark summary generation on top of the JSON benchmark artifacts, Stage 16C adds benchmark result analysis on top of those local outputs, Stage 16D finalizes the Performance / Load Testing workflow as PR-ready documentation, Stage 17A adds the first formal raw-data contract foundation for stronger future validation, Stage 17B adds local raw-CSV contract validation tooling on top of that foundation, Stage 17C makes that validation a real Airflow pipeline guardrail before producer execution, Stage 18A adds a local rule-based anomaly detection foundation, Stage 18B persists those anomaly results into the warehouse, Stage 18C integrates that anomaly detection into Airflow after warehouse loading, Stage 18D finalizes the documentation and validation story for PR review, Stage 21A adds a reusable local smoke-test foundation that prepares future fuller dataset validation without changing any existing pipeline runtime logic, and Stage 21B extends that entry point with a controlled sample runtime E2E pass while leaving full or `100k` scale validation for later Stage 21C / Stage 21D work.
