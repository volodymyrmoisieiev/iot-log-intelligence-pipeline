# IoT Log Intelligence Pipeline

## 1. Project overview

IoT Log Intelligence Pipeline is a portfolio project focused on end-to-end data engineering for IoT logs: ingestion, processing, storage, transformation, and analytics.

The repository is currently at Stage 6B, with a working local Kafka stack, a Go producer, a Python consumer validation layer, a local PostgreSQL warehouse foundation, a warehouse loader service, dbt staging models, dbt analytics marts on top of PostgreSQL, and a Streamlit dashboard with KPIs, charts, and filters.

## 2. Planned local architecture

```text
Raw logs -> Go Producer -> Kafka -> Python Consumer -> Kafka processed/invalid topics -> warehouse loader -> PostgreSQL -> dbt -> Streamlit dashboard
```

Local MVP focus:

- Ingest or simulate IoT log events.
- Publish events with a Go producer.
- Consume and validate them in Python.
- Store curated data in a local warehouse or data lake layer.
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
- Stage 8: PySpark
- Stage 9: AWS + Terraform
- Stage 10: CI/CD + final docs

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

## 16. Security note

Do not commit real credentials, production secrets, or sensitive data. Use environment variables and secret management outside the repository.

## Current stage

Stage 6B includes:

- repository skeleton and documentation
- local Docker Compose services for Kafka, Kafka topic initialization, and Kafka UI
- a Go producer that reads sample CSV data and publishes JSON messages to Kafka
- a Python consumer that validates and routes records to processed and invalid Kafka topics
- a local PostgreSQL foundation with automatic table initialization for processed and invalid IoT logs
- a warehouse loader that consumes processed and invalid Kafka topics and writes to PostgreSQL
- a dbt project with PostgreSQL sources, staging tables, and baseline tests
- analytics marts for device risk, attack summary, protocol metrics, and pipeline quality
- a Streamlit dashboard with KPI cards, filters, charts, and mart tables
- safe local environment placeholders

Airflow, Spark, AWS, Terraform, CI/CD, and advanced dashboard features are intentionally not implemented yet and will be added in later stages.
