# Screenshots

Screenshots for docs, demos, dashboards, and architecture walkthroughs go here.

Recommended Stage 6 dashboard screenshots:

- `dashboard-overview.png` - full dashboard landing view
- `pipeline-kpis.png` - Pipeline Overview section with KPI cards
- `device-risk-section.png` - Device Risk charts and table
- `attack-protocol-charts.png` - Attack Summary and Protocol Metrics charts

These files are optional placeholders for portfolio documentation. Do not add screenshots unless you actually capture them during local verification.

## Airflow screenshots checklist

Recommended Stage 7 Airflow screenshots:

- `airflow-dag-list.png` - DAG list showing both `iot_pipeline_smoke_dag` and `iot_local_pipeline_dag`
- `airflow-local-pipeline-graph.png` - Graph view of `iot_local_pipeline_dag`
- `airflow-local-pipeline-success.png` - successful `iot_local_pipeline_dag` run
- `airflow-run-dbt-test-logs.png` - successful `run_dbt_test` task logs showing `53` tests passing
- `airflow-homepage.png` - optional Airflow UI homepage on port `8081`

Capture suggestions:

- use [http://localhost:8081](http://localhost:8081/) for Airflow screenshots
- log in with `airflow / airflow`
- prefer screenshots after a successful DAG run so task states are clearly visible
- keep filenames stable so future README references stay easy to maintain

## Spark verification checklist

Recommended Stage 9B verification artifacts:

- terminal output showing `docker compose run --build --rm spark-batch`
- terminal output showing `docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py`
- terminal output or file listing showing `data/processed/spark/device_features`

Checklist:

- confirm the smoke job completes successfully
- confirm the device feature job reads `72` sample rows
- confirm the device feature job writes `24` device-level feature rows
- confirm the output directory contains Parquet output files
