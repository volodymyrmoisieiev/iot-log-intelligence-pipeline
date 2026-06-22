# Dashboard

Stage 14E extends the Streamlit dashboard with a pipeline observability section while preserving the existing analytics UI for the IoT Log Intelligence Pipeline.

## Files

- `app.py` - Streamlit entrypoint with analytics sections and the new `Pipeline Monitoring` section
- `db.py` - reusable PostgreSQL connection helpers
- `requirements.txt` - Python dependencies for the dashboard image
- `Dockerfile` - container image for the local Streamlit service

## Dashboard purpose

This dashboard is the local analytics UI for the project. It connects to PostgreSQL, reads dbt marts from the `public` schema, and also reads observability tables so demos can show both analytics and pipeline monitoring in one place.

## Sections

- `Pipeline Overview` - KPI cards and one-row mart preview from `mart_pipeline_quality_summary`
- `Device Risk` - risk distribution, top devices by event volume, and ranked device risk table from `mart_device_risk_summary`
- `Attack Summary` - attack volume chart and summary table from `mart_attack_summary`
- `Protocol Metrics` - protocol traffic chart and summary table from `mart_protocol_metrics`
- `Pipeline Monitoring` - latest pipeline run summary, recent run history, latest-run quality checks, and recent alerts from `pipeline_run_audit`, `pipeline_quality_checks`, and `pipeline_alerts`
- `Raw Mart Tables` - filtered previews of all current marts for validation and screenshots

If PostgreSQL is unavailable or dbt marts have not been built yet, the app shows friendly warnings instead of crashing.
If observability tables are empty, the `Pipeline Monitoring` section shows a friendly message instead of crashing.

## Before opening the dashboard

Prepare the data first:

- start Kafka, PostgreSQL, and topic initialization services
- run the Go producer
- run the Python consumer
- run the warehouse loader
- build dbt marts with `dbt run`
- validate marts with `dbt test`
- optionally generate observability rows with the local writer or Airflow DAG

If these steps are skipped, the dashboard will still open, but sections may show empty-state guidance instead of data.

## Local run flow

Start the local pipeline and build dbt marts before opening the dashboard:

```bash
docker compose config
docker compose up -d kafka kafka-ui kafka-init postgres
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage6c-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage6c-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose run --build --rm observability-writer --run-id dashboard-monitoring-demo --publish-alerts --min-processed-records 999999
docker compose build streamlit-dashboard
docker compose up -d --force-recreate streamlit-dashboard
curl -I http://localhost:8501
```

Open the dashboard at [http://localhost:8501](http://localhost:8501/).

Find the `Pipeline Monitoring` section below the analytics charts and above the raw mart tables.

You can also generate observability rows through Airflow by triggering `iot_local_pipeline_dag`, which now runs the observability writer near the end of the DAG.

## Portfolio screenshots

Recommended screenshots can be stored in [docs/screenshots/README.md](<C:\Users\User\Desktop\Cloud Technologies\IoT_Log_Intelligence_Pipeline\docs\screenshots\README.md>) under `docs/screenshots/`.

Suggested captures:

- dashboard overview
- pipeline KPI section
- device risk section
- attack and protocol charts
- pipeline monitoring latest run summary
- pipeline monitoring recent alerts table
