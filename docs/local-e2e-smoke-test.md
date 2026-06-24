# Local E2E Smoke Test

## Validation Modes

There are now three related local validation modes:

- `dry-run` checks which smoke-test commands would execute and writes that plan into the JSON summary without running the external command steps
- the default smoke test validates repository structure, selected dataset availability, Docker Compose config, Python syntax, Terraform validation, data-contract validation, and optional read-only anomaly detection without starting the full local pipeline
- `--run-profile-pipeline` adds a controlled profile-specific runtime E2E pass that starts only the required local services, uses bounded row/message limits, and verifies PostgreSQL row-count deltas after producer, consumer, and warehouse-loader execution

`--run-sample-pipeline` is still supported as a backward-compatible alias for `--profile sample --run-profile-pipeline`.

## What It Is

The local E2E smoke test is a safe repository-level validation helper that checks whether the project still looks runnable as one system without starting the full Kafka, PostgreSQL, Airflow, dbt, or AWS workflow by default.

Its goal is fast confidence, not full data processing. It validates the repository foundation, selected dataset profile, Docker Compose configuration, Python syntax, Terraform configuration, and optionally reuses the existing data-contract and anomaly-detection helpers in bounded, read-only-friendly ways.

The script lives at `scripts/run_local_e2e_smoke_test.py` and uses only the Python standard library.

For the final Stage 21 runbook, validated full-run summary, bottleneck notes, and PR-ready cleanup guidance, see [docs/stage-21-local-e2e-validation.md](docs/stage-21-local-e2e-validation.md).

## Why This Is Not a Full Dataset Run

The smoke test keeps `sample` as the default profile and caps row-oriented dataset checks with `--max-rows` so normal local validation stays fast, predictable, and inexpensive.

That means:

- it does not start the full Kafka/PostgreSQL pipeline by default
- it does not run the full dataset by default
- it does not deploy AWS resources
- it does not run `terraform apply`

This keeps the foundation safe for routine pre-PR validation while still surfacing obvious repository-level regressions.

## Dry-Run Example

Use dry-run when you want to confirm which checks would execute without running the external command steps:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --dry-run --output-json docs/e2e-smoke-test-local.json
```

Dry-run still records the selected profile, row cap, and planned checks in the JSON summary.

## Default Smoke-Test Example

Use the default `sample` profile for the fastest safe repository-wide smoke test:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --output-json docs/e2e-smoke-test-local.json
```

What the script checks:

- expected repository folders and key files
- selected dataset file existence
- `docker compose config`
- Python syntax for lightweight scripts and, unless skipped, Airflow DAG files
- Terraform init/validate for `infra/aws-orchestration` unless skipped
- bounded data contract validation against up to `--max-rows` rows
- anomaly detection in read-only helper mode when practical, or `skip` if the local runtime is not ready

Useful optional flags:

- `--skip-airflow`
- `--skip-dbt`
- `--skip-anomaly-detection`
- `--skip-terraform`

## Controlled Sample Runtime E2E Example

Use either `--run-profile-pipeline` or the backward-compatible `--run-sample-pipeline` alias when you want the script to go beyond repository-level checks and run a bounded local sample pipeline flow:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

What this mode adds:

- starts the required Docker Compose services for Kafka and PostgreSQL
- creates isolated Kafka topics for the controlled runtime pass
- runs the Go producer with `DATASET_PROFILE=sample`, `PRODUCER_MAX_ROWS`, `PRODUCER_PROGRESS_INTERVAL`, and `PRODUCER_SEND_DELAY_MS=0`
- runs the Python consumer with matching `CONSUMER_MAX_MESSAGES` and `CONSUMER_PROGRESS_INTERVAL`
- runs the warehouse loader with matching `WAREHOUSE_LOADER_MAX_MESSAGES`, `WAREHOUSE_LOADER_PROGRESS_INTERVAL`, and `WAREHOUSE_LOADER_BATCH_SIZE`
- captures PostgreSQL row counts before and after the run and verifies the row-count delta
- reruns bounded data-contract validation
- attempts anomaly detection in safe read-only mode and records either a result or an explicit `skip`

How progress is displayed in this mode:

- the Go producer prints interval-based progress lines such as attempted, sent, and failed counts
- the Python consumer and warehouse loader use `tqdm` only when it is installed and the process is attached to a real terminal
- when `tqdm` is unavailable or output is being captured, the Python components fall back to regular interval-based log lines
- the JSON summary records the effective per-component progress intervals under `profile_pipeline_progress`
- the warehouse-loader entry in that summary now also records the effective `batch_size`

This still does not run `terraform apply`, deploy AWS resources, or switch the repository to a full-dataset validation path.

## Controlled Medium Runtime E2E Example

Use the `medium` profile when you want a richer but still bounded local runtime validation on a prepared subset such as `10000` rows:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile medium --max-rows 10000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

The script now performs a dataset preflight for the selected profile and reports:

- the resolved dataset path
- whether the dataset exists
- the available row count when the file is present
- the bounded row count that will be used for checks and runtime flow

If `data/processed/medium_iot_logs.csv` is missing, the script fails clearly and tells you how to generate it:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

## Controlled Full Runtime E2E Example

Use the `full` profile only for an intentional larger validation run, and only with the explicit `--allow-full-run` safeguard:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

Why `--allow-full-run` is required:

- a full-profile run is heavier and slower than sample or medium validation
- it is easier to consume more local CPU, Docker, Kafka, and PostgreSQL time by mistake
- it should remain an intentional action rather than the default local workflow

The script performs stronger full-run preflight checks before runtime execution:

- resolves the expected full dataset path
- fails clearly if `data/raw/full_iot_logs.csv` is missing
- counts available rows
- fails clearly if fewer rows are available than requested by `--max-rows`

If the full dataset is missing, place it at:

```text
data/raw/full_iot_logs.csv
```

Expected success for the controlled full run means:

- producer passed
- consumer passed
- warehouse loader passed
- PostgreSQL delta equals the expected bounded row count
- data contract validation passed
- anomaly detection passed, or reports a clear skip/failure reason in the JSON summary

The JSON report now records stage durations for:

- data contract validation
- producer
- consumer
- warehouse loader
- PostgreSQL verification
- anomaly detection

It also records the effective progress configuration for the controlled runtime flow:

- `producer.progress_interval`
- `consumer.progress_interval`
- `warehouse_loader.progress_interval`
- `warehouse_loader.batch_size`
- Python progress mode as `tqdm_if_tty_else_log`

Expected runtime note:

- a `100000`-row run can take noticeably longer than sample or medium validation
- Docker health, Kafka throughput, and local PostgreSQL performance will affect total runtime
- `PRODUCER_SEND_DELAY_MS=0` stays enforced for controlled runtime validation so the producer does not add artificial delay
- progress intervals default to `1000`, and the smoke-test helper may lower the interval for smaller bounded runs so at least one visible progress update is still emitted
- `WAREHOUSE_LOADER_BATCH_SIZE` defaults to `1000` for the controlled runtime flow so medium and full runs can compare loader timing more meaningfully
- Stage 22B adds batch insert optimization on top of the Stage 22A progress visibility foundation

## Comparing Before And After Runtime

To compare warehouse-loader behavior before and after this optimization:

- run the same `medium` or `full` command with the same `--max-rows`
- compare `stage_durations_seconds.warehouse_loader` in the generated JSON summaries
- keep `WAREHOUSE_LOADER_BATCH_SIZE` fixed first, then vary it intentionally if you want to test different tradeoffs

## Why `full` Is Not the Default

The `full` profile is intentionally heavier, environment-dependent, and slower. Making it the default would turn a quick safety check into a long and fragile validation path.

Keeping `sample` as the default helps preserve:

- fast feedback during local iteration
- lower resource usage on developer machines
- safer validation before intentional larger-scale runs

Stage 21D adds controlled `full` validation up to `100000` rows, but keeps it behind `--allow-full-run` so it stays intentional and never becomes the default path.

## How This Prepares Future Full Validation

This smoke-test foundation establishes a reusable entry point, consistent JSON reporting, bounded dataset inspection, and safe Terraform/data-contract/anomaly checks that future stages can extend into a fuller local-system validation workflow.

With Stage 21D, that foundation now includes isolated sample, medium, and controlled full runtime flows with bounded producer, consumer, and loader execution, while still preserving safe defaults for normal local development.
