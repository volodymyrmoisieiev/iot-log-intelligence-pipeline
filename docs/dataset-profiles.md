# Dataset Profiles

## Overview

Stage 15A adds a dataset profile foundation so later stages can switch between documented dataset sizes without changing the repository's default lightweight behavior.

The current pipeline still uses the tracked sample CSV at `data/samples/sample_iot_logs.csv`. This stage does not change the Go producer, Python consumer, warehouse loader, Airflow DAGs, or dbt models yet. It only defines the profile contract those later stages can reuse.

Profile metadata lives in `data/dataset_profiles.yml`.

## Profiles

### `sample`

- path: `data/samples/sample_iot_logs.csv`
- purpose: quick local demo runs, smoke checks, and CI-safe validation
- git policy: committed to the repository
- CI safety: safe for CI because it is intentionally tiny and predictable

Use `sample` when you want the fastest feedback loop and when repository validation must stay small, deterministic, and inexpensive.

### `medium`

- path: `data/processed/medium_iot_logs.csv`
- purpose: integration testing on a larger but still curated subset
- git policy: not committed by default
- CI safety: not considered CI-safe unless a future workflow explicitly documents and provisions it

Use `medium` when the sample dataset is too small to exercise realistic record volume, but a full raw-dataset run would be too heavy for normal local iteration.

Create it locally with the dataset preparation script:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000
```

### `full`

- path: `data/raw/full_iot_logs.csv`
- purpose: full raw-dataset processing for local/manual or cloud-style validation
- git policy: never commit the raw full dataset to normal repository history
- CI safety: not safe for CI

Use `full` only for intentional larger-scale validation. It is expected to be slower, heavier, and more environment-specific than the tracked sample workflow.

## Why the Modes Exist

`sample` exists so demos, smoke tests, and CI checks stay fast and stable.

`medium` exists so future integration validation can exercise more realistic data volume without requiring the whole raw dataset.

`full` exists so future end-to-end validation can be tested on the complete source data when a developer or cloud-style environment is ready for that cost.

## Git and Data Governance

Large raw and generated datasets should not be committed because they make clones heavier, increase review noise, and create avoidable repository churn.

The repository keeps only the tiny sample CSV under version control. Planned future larger inputs such as `data/processed/medium_iot_logs.csv` and `data/raw/full_iot_logs.csv` are intentionally ignored by git.

That means you can generate or copy local larger files for testing without polluting normal commits, pull requests, or repository history.

## Dataset Preparation Script

Stage 15B adds `scripts/create_dataset_profile.py`, a local helper that prepares a smaller CSV from a larger IoT CSV input.

What it does:

- reads a source CSV file
- validates that the required IoT columns exist
- writes up to `N` rows to a target CSV
- preserves the original header row
- creates the output parent directory if needed
- refuses to overwrite an existing file unless `--overwrite` is passed

Required columns:

- `event_timestamp`
- `device_id`
- `source_ip`
- `destination_ip`
- `protocol`
- `packet_size`
- `duration_ms`
- `event_type`
- `attack_type`
- `status`

Supported selection modes:

- `first` writes the first `N` data rows after the header
- `random` writes a reproducible random subset of up to `N` rows using `--seed`

Use `first` when you want deterministic slices that are easy to inspect manually.

Use `random` when you want a broader subset from a larger raw file without taking only the earliest rows.

## Example Commands

Create the planned `medium` dataset from a larger local raw CSV:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000
```

Create a reproducible random `medium` dataset with a custom seed:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --mode random --seed 7
```

Replace an existing generated `medium` file intentionally:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

## Go Producer Profile Usage

Stage 15C adds dataset profile support to the Go producer while keeping the default sample behavior unchanged.

Producer behavior:

- default profile is `sample`
- `PRODUCER_INPUT_FILE` still works and overrides `DATASET_PROFILE`
- `PRODUCER_MAX_ROWS=0` means no limit
- positive `PRODUCER_MAX_ROWS` values cap how many records are read and published

Producer profile mapping:

- `sample` -> `/app/data/samples/sample_iot_logs.csv`
- `medium` -> `/app/data/processed/medium_iot_logs.csv`
- `full` -> `/app/data/raw/full_iot_logs.csv`

PowerShell examples:

Default sample producer run:

```powershell
docker compose up -d kafka kafka-ui kafka-init
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
```

Sample profile with a row cap:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=sample -e PRODUCER_SEND_DELAY_MS=0 -e PRODUCER_MAX_ROWS=5 go-producer
```

Medium profile after generating `data/processed/medium_iot_logs.csv`:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=medium -e PRODUCER_SEND_DELAY_MS=0 -e PRODUCER_MAX_ROWS=5 go-producer
```

Full profile warning:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=full -e PRODUCER_SEND_DELAY_MS=0 go-producer
```

Use `full` only for intentional manual or larger-scale validation after placing the raw file at `data/raw/full_iot_logs.csv`.

## Consumer and Loader Safety for Larger Runs

Stage 15D keeps the existing consumer and warehouse-loader behavior but adds clearer progress and summary logging for larger validation runs.

Useful controls:

- `CONSUMER_MAX_MESSAGES` and `WAREHOUSE_LOADER_MAX_MESSAGES` keep medium or full validation runs bounded
- `CONSUMER_PROGRESS_INTERVAL` and `WAREHOUSE_LOADER_PROGRESS_INTERVAL` control how often progress logs are printed
- unique `CONSUMER_GROUP_ID` and `WAREHOUSE_LOADER_GROUP_ID` values help you run repeatable validation passes without reusing an older Kafka group state

PowerShell examples:

Consumer on a bounded larger run:

```powershell
docker compose run --build --rm `
  -e CONSUMER_GROUP_ID=stage15d-consumer `
  -e CONSUMER_MAX_MESSAGES=10 `
  -e CONSUMER_PROGRESS_INTERVAL=5 `
  python-consumer
```

Warehouse loader on a bounded larger run:

```powershell
docker compose run --build --rm `
  -e WAREHOUSE_LOADER_GROUP_ID=stage15d-loader `
  -e WAREHOUSE_LOADER_MAX_MESSAGES=10 `
  -e WAREHOUSE_LOADER_PROGRESS_INTERVAL=5 `
  warehouse-loader
```

For `full` dataset mode, keep these runs manual, local, or cloud-style. They are not intended to be CI-safe.

## Airflow Dataset Mode Integration

Stage 15E wires dataset-mode settings into `airflow/dags/iot_local_pipeline_dag.py` while keeping the default DAG run sample-safe.

Airflow DAG defaults:

- `DATASET_PROFILE=sample`
- `PRODUCER_MAX_ROWS=0`
- `PRODUCER_SEND_DELAY_MS=0`
- `CONSUMER_MAX_MESSAGES=72`
- `WAREHOUSE_LOADER_MAX_MESSAGES=72`
- `CONSUMER_PROGRESS_INTERVAL=1000`
- `WAREHOUSE_LOADER_PROGRESS_INTERVAL=1000`

That means the default Airflow run still behaves like the current tracked sample pipeline.

### Prepare a medium dataset

```powershell
& 'C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe' .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

### Set environment variables before starting Airflow

```powershell
$env:DATASET_PROFILE = "medium"
$env:PRODUCER_MAX_ROWS = "1000"
$env:CONSUMER_MAX_MESSAGES = "1000"
$env:WAREHOUSE_LOADER_MAX_MESSAGES = "1000"
$env:CONSUMER_PROGRESS_INTERVAL = "250"
$env:WAREHOUSE_LOADER_PROGRESS_INTERVAL = "250"
```

Recommended values:

- sample mode: keep `DATASET_PROFILE=sample`, `PRODUCER_MAX_ROWS=0`, `CONSUMER_MAX_MESSAGES=72`, `WAREHOUSE_LOADER_MAX_MESSAGES=72`
- medium mode: keep producer and consumer/loader row caps aligned so all three steps expect the same batch size

### Airflow commands

List DAGs:

```powershell
docker compose run --rm airflow-webserver airflow dags list
```

List DAG tasks:

```powershell
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
```

Trigger the DAG manually:

```powershell
docker compose run --rm airflow-webserver airflow dags trigger iot_local_pipeline_dag
```

Validate row counts after a run:

```powershell
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS processed_rows FROM processed_iot_logs;"
docker exec -e PGPASSWORD=iot_password -i iot-postgres psql -U iot_user -d iot_logs -P pager=off -c "SELECT COUNT(*) AS invalid_rows FROM invalid_iot_logs;"
```

### Full mode warning

Use `full` mode only for manual local or cloud-style validation after the full dataset is present. It should not be used in CI, and it should not be part of routine lightweight Airflow demo runs.

## How Future Stages Will Use This

Later Stage 15 work can wire these documented profiles into:

- dataset preparation and local profile generation workflows
- Go producer input selection
- Python consumer and warehouse-loader validation runs
- Airflow orchestration options
- larger local or cloud-style dataset processing flows

Stage 15A defined the shared profile contract, Stage 15B added local dataset preparation, Stage 15C wired profiles into the Go producer, Stage 15D added safer consumer and loader controls, and Stage 15E carries those dataset settings into the local Airflow DAG and runbook.

## Validation Commands

Verify the profile file and tracked sample dataset exist:

```powershell
Test-Path .\data\samples\sample_iot_logs.csv
Test-Path .\data\dataset_profiles.yml
```

Validate the script safely with the tracked sample file:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\samples\sample_iot_logs.csv --output .\data\processed\medium_iot_logs.csv --rows 10 --overwrite
Test-Path .\data\processed\medium_iot_logs.csv
Get-Content .\data\processed\medium_iot_logs.csv -TotalCount 3
git check-ignore -v .\data\processed\medium_iot_logs.csv
```

Delete the generated validation file after the check:

```powershell
Remove-Item .\data\processed\medium_iot_logs.csv
```

Verify the planned larger dataset paths are ignored by git:

```powershell
git check-ignore -v .\data\raw\full_iot_logs.csv
git check-ignore -v .\data\processed\medium_iot_logs.csv
```

If `git check-ignore` does not report a result for a missing path in your environment, create temporary empty files, verify the ignore rules, then delete them:

```powershell
New-Item -ItemType File -Path .\data\raw\full_iot_logs.csv -Force
New-Item -ItemType File -Path .\data\processed\medium_iot_logs.csv -Force
git check-ignore -v .\data\raw\full_iot_logs.csv
git check-ignore -v .\data\processed\medium_iot_logs.csv
Remove-Item .\data\raw\full_iot_logs.csv
Remove-Item .\data\processed\medium_iot_logs.csv
```

Confirm validation does not leave repository noise behind:

```powershell
git status
```
