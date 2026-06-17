# Dashboard

Stage 6B upgrades the Streamlit dashboard into a more useful analytics UI for the IoT Log Intelligence Pipeline.

## Files

- `app.py` - Streamlit entrypoint with KPIs, filters, charts, and mart tables
- `db.py` - reusable PostgreSQL connection helpers
- `requirements.txt` - Python dependencies for the dashboard image
- `Dockerfile` - container image for the local Streamlit service

## What this dashboard does

- connects to PostgreSQL with environment variables
- reads existing dbt marts from the `public` schema
- shows connection status
- renders KPI cards from `mart_pipeline_quality_summary`
- adds sidebar filters for:
  - `risk_level`
  - `protocol`
  - `attack_type`
  - `top N`
- shows simple charts for:
  - device count by `risk_level`
  - top devices by `total_events`
  - attack events by `attack_type`
  - total events by `protocol`
- displays sorted mart tables for:
  - `mart_device_risk_summary`
  - `mart_attack_summary`
  - `mart_protocol_metrics`
  - `mart_pipeline_quality_summary`

If PostgreSQL is unavailable or dbt marts have not been built yet, the app shows friendly warnings instead of crashing.

## Local run flow

Start the local pipeline and build dbt marts before opening the dashboard:

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
