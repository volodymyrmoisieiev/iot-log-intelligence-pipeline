# IoT Log Intelligence Pipeline

## 1. Project overview

IoT Log Intelligence Pipeline is a portfolio project focused on end-to-end data engineering for IoT logs: ingestion, processing, storage, transformation, and analytics.

The repository is currently at Stage 0, so it contains structure, documentation, and config scaffolding only.

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

## 7. Security note

Do not commit real credentials, production secrets, or sensitive data. Use environment variables and secret management outside the repository.

## Current stage

Stage 0 includes only:

- repository skeleton
- documentation
- configuration scaffolding

No pipeline logic, runnable services, or infrastructure code has been added yet.
