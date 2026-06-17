# Dashboard

Stage 6A adds the Streamlit dashboard foundation for the IoT Log Intelligence Pipeline.

## Files

- `app.py` - Streamlit entrypoint with basic KPIs and mart previews
- `db.py` - reusable PostgreSQL connection helpers
- `requirements.txt` - Python dependencies for the dashboard image
- `Dockerfile` - container image for the local Streamlit service

## What this dashboard does

- connects to PostgreSQL with environment variables
- reads existing dbt marts from the `public` schema
- shows connection status
- renders basic KPI metrics from `mart_pipeline_quality_summary`
- previews:
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
docker compose run --build --rm -e CONSUMER_GROUP_ID=stage6a-valid -e CONSUMER_MAX_MESSAGES=72 python-consumer
docker compose run --build --rm -e WAREHOUSE_LOADER_GROUP_ID=stage6a-loader -e WAREHOUSE_LOADER_MAX_MESSAGES=72 warehouse-loader
docker compose run --build --rm dbt dbt run
docker compose run --build --rm dbt dbt test
docker compose up --build streamlit-dashboard
```

Open the dashboard at [http://localhost:8501](http://localhost:8501/).
