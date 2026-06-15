# IoT Log Intelligence Pipeline

## 1. Project overview

IoT Log Intelligence Pipeline is a portfolio project focused on end-to-end data engineering for IoT logs: ingestion, processing, storage, transformation, and analytics.

The repository is currently at Stage 3, with a working local Kafka stack, a Go producer, and a Python consumer validation layer.

## 2. Planned local architecture

```text
Raw logs -> Go Producer -> Kafka -> Python Consumer -> warehouse/data lake -> dbt -> Streamlit dashboard
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
|-- sql/
|-- tests/
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

## 10. Security note

Do not commit real credentials, production secrets, or sensitive data. Use environment variables and secret management outside the repository.

## Current stage

Stage 3 includes:

- repository skeleton and documentation
- local Docker Compose services for Kafka, Kafka topic initialization, and Kafka UI
- a Go producer that reads sample CSV data and publishes JSON messages to Kafka
- a Python consumer that validates and routes records to processed and invalid Kafka topics
- safe local environment placeholders

Storage, dbt, Airflow, Spark, AWS, Terraform, and analytics layers are intentionally not implemented yet and will be added in later stages.
