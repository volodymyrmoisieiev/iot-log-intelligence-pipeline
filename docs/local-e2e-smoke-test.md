# Local E2E Smoke Test

## Validation Modes

There are now three related local validation modes:

- `dry-run` checks which smoke-test commands would execute and writes that plan into the JSON summary without running the external command steps
- the default smoke test validates repository structure, selected dataset availability, Docker Compose config, Python syntax, Terraform validation, data-contract validation, and optional read-only anomaly detection without starting the full local pipeline
- `--run-sample-pipeline` adds a controlled sample runtime E2E pass that starts only the required local services, uses bounded row/message limits, and verifies PostgreSQL row-count deltas after producer, consumer, and warehouse-loader execution

## What It Is

The local E2E smoke test is a safe repository-level validation helper that checks whether the project still looks runnable as one system without starting the full Kafka, PostgreSQL, Airflow, dbt, or AWS workflow by default.

Its goal is fast confidence, not full data processing. It validates the repository foundation, selected dataset profile, Docker Compose configuration, Python syntax, Terraform configuration, and optionally reuses the existing data-contract and anomaly-detection helpers in bounded, read-only-friendly ways.

The script lives at `scripts/run_local_e2e_smoke_test.py` and uses only the Python standard library.

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

Use `--run-sample-pipeline` when you want the script to go beyond repository-level checks and run a bounded local sample pipeline flow:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-sample-pipeline --output-json docs/e2e-smoke-test-local.json
```

What this mode adds:

- starts the required Docker Compose services for Kafka and PostgreSQL
- creates isolated Kafka topics for the controlled runtime pass
- runs the Go producer with `DATASET_PROFILE=sample`, `PRODUCER_MAX_ROWS`, and `PRODUCER_SEND_DELAY_MS=0`
- runs the Python consumer with matching `CONSUMER_MAX_MESSAGES`
- runs the warehouse loader with matching `WAREHOUSE_LOADER_MAX_MESSAGES`
- captures PostgreSQL row counts before and after the run and verifies the row-count delta
- reruns bounded data-contract validation
- attempts anomaly detection in safe read-only mode and records either a result or an explicit `skip`

This still does not run `terraform apply`, deploy AWS resources, or switch the repository to a full-dataset validation path.

## Why `full` Is Not the Default

The `full` profile is intentionally heavier, environment-dependent, and slower. Making it the default would turn a quick safety check into a long and fragile validation path.

Keeping `sample` as the default helps preserve:

- fast feedback during local iteration
- lower resource usage on developer machines
- safer validation before intentional larger-scale runs

Stage 21B keeps the controlled runtime path focused on the tracked sample dataset. Wider `full` or `100k` style validation is deferred to later Stage 21C and Stage 21D work.

## How This Prepares Future Full Validation

This smoke-test foundation establishes a reusable entry point, consistent JSON reporting, bounded dataset inspection, and safe Terraform/data-contract/anomaly checks that future stages can extend into a fuller local-system validation workflow.

With Stage 21B, that foundation now also includes an isolated sample-runtime flow with bounded producer, consumer, and loader execution. Later Stage 21C and Stage 21D work can build on the same entry point for fuller dataset-scale validation without changing the current runtime behavior of existing pipeline components.
