# Local E2E Smoke Test

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

## Sample Profile Example

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

## Why `full` Is Not the Default

The `full` profile is intentionally heavier, environment-dependent, and slower. Making it the default would turn a quick safety check into a long and fragile validation path.

Keeping `sample` as the default helps preserve:

- fast feedback during local iteration
- lower resource usage on developer machines
- safer validation before intentional larger-scale runs

## How This Prepares Future Full Validation

This smoke-test foundation establishes a reusable entry point, consistent JSON reporting, bounded dataset inspection, and safe Terraform/data-contract/anomaly checks that future stages can extend into a fuller local-system validation workflow.

When the repository is ready for heavier orchestration, this foundation can be expanded to cover more intentional medium/full-profile validation without changing the current runtime behavior of existing pipeline components.
