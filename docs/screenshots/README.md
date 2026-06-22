# Screenshots

Screenshots for docs, demos, dashboards, and architecture walkthroughs go here.

Recommended Stage 6 dashboard screenshots:

- `dashboard-overview.png` - full dashboard landing view
- `pipeline-kpis.png` - Pipeline Overview section with KPI cards
- `device-risk-section.png` - Device Risk charts and table
- `attack-protocol-charts.png` - Attack Summary and Protocol Metrics charts
- `pipeline-monitoring-summary.png` - Pipeline Monitoring latest run summary cards
- `pipeline-monitoring-alerts.png` - Pipeline Monitoring recent alerts table

These files are optional placeholders for portfolio documentation. Do not add screenshots unless you actually capture them during local verification.

Suggested Stage 14E observability dashboard captures:

- dashboard view with both analytics sections and `Pipeline Monitoring`
- latest run summary showing `run_id`, status, processed and invalid counts, invalid rate, and total alerts
- latest-run quality checks table
- recent alerts table with `is_published_to_kafka`

## Airflow screenshots checklist

Recommended Stage 7 Airflow screenshots:

- `airflow-dag-list.png` - DAG list showing both `iot_pipeline_smoke_dag` and `iot_local_pipeline_dag`
- `airflow-local-pipeline-graph.png` - Graph view of `iot_local_pipeline_dag`
- `airflow-local-pipeline-success.png` - successful `iot_local_pipeline_dag` run
- `airflow-run-dbt-test-logs.png` - successful `run_dbt_test` task logs showing `53` tests passing
- `airflow-homepage.png` - optional Airflow UI homepage on port `8081`

Recommended Stage 10 Airflow + Spark screenshots:

- `airflow-local-pipeline-graph-spark.png` - DAG graph showing `run_spark_device_features` and `validate_spark_device_features_output`
- `airflow-local-pipeline-task-list-spark.png` - Airflow task list for `iot_local_pipeline_dag` including `validate_spark_device_features_output`
- `airflow-local-pipeline-success-spark.png` - successful DAG run with both Spark-related tasks marked `success`
- `airflow-run-spark-device-features-logs.png` - logs for `run_spark_device_features`
- `airflow-validate-spark-output-logs.png` - logs for `validate_spark_device_features_output`

Recommended Stage 11C Airflow + MinIO screenshots:

- `airflow-local-pipeline-graph-minio.png` - DAG graph showing `start_object_storage`, `upload_spark_features_to_minio`, and `validate_minio_spark_features_upload`
- `airflow-local-pipeline-task-list-minio.png` - Airflow task list including the three MinIO-related tasks
- `airflow-upload-spark-features-to-minio-logs.png` - logs for `upload_spark_features_to_minio`
- `airflow-validate-minio-upload-logs.png` - logs for `validate_minio_spark_features_upload`

Recommended Stage 15E dataset-mode Airflow screenshots:

- `airflow-local-pipeline-medium-env.png` - Airflow task logs showing medium-profile env values or row caps
- `airflow-consumer-progress-medium.png` - consumer task logs with progress updates during a medium-style bounded run
- `airflow-loader-progress-medium.png` - warehouse loader task logs with progress updates during a medium-style bounded run

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

Recommended Stage 10 verification artifacts:

- screenshot of `data/processed/spark/device_features` after a successful DAG run
- screenshot or terminal output showing at least one `part-*.parquet` file
- optional screenshot of the Airflow graph or grid after the validation task succeeds

## MinIO screenshots checklist

Recommended Stage 11A object-storage screenshots:

- `minio-console-home.png` - MinIO console landing page on port `9001`
- `minio-bucket-iot-data-lake.png` - bucket browser showing `iot-data-lake`
- `minio-object-storage-services.png` - terminal output for `docker compose ps` with `iot-minio` visible

Recommended Stage 11B upload screenshots:

- `minio-spark-device-features-prefix.png` - bucket browser showing `spark/device_features/latest/`
- `object-storage-uploader-success.png` - terminal output for `docker compose --profile object-storage run --build --rm object-storage-uploader`
- `spark-device-features-local-output.png` - local `data/processed/spark/device_features` folder before upload

Capture suggestions:

- use [http://localhost:9001](http://localhost:9001/) for the MinIO console
- sign in with `minioadmin / minioadmin`
- capture screenshots only after `minio-init` completes successfully
- for Stage 11B, capture the prefix view after a successful upload
- for Stage 11C, capture both the Airflow task success state and the MinIO prefix view
- keep filenames stable so future README references stay easy to maintain

## CI screenshots checklist

Optional Stage 12C verification artifacts:

- `github-actions-terraform-validation.png` - GitHub Actions job list showing `Terraform Validation`
- `github-actions-terraform-validation-logs.png` - logs for `terraform fmt -check -recursive`, `terraform init -backend=false`, and `terraform validate`

Notes:

- AWS credentials are not required for this CI validation job
- CI does not run `terraform plan` or `terraform apply`
- capture screenshots only if you later verify the workflow in GitHub Actions

Checklist:

- confirm the smoke job completes successfully
- confirm the device feature job reads `72` sample rows
- confirm the device feature job writes `24` device-level feature rows
- confirm the output directory contains Parquet output files
- confirm the Airflow DAG includes `run_spark_device_features`
- confirm the Airflow DAG includes `validate_spark_device_features_output`
- confirm the Airflow DAG includes `start_object_storage`
- confirm the Airflow DAG includes `upload_spark_features_to_minio`
- confirm the Airflow DAG includes `validate_minio_spark_features_upload`
