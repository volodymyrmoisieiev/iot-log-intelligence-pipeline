# Local E2E Smoke Test

## Validation Modes

There are now four related local validation modes:

- `dry-run` checks which smoke-test commands would execute and writes that plan into the JSON summary without running the external command steps
- the default smoke test validates repository structure, selected dataset availability, Docker Compose config, Python syntax, Terraform validation, data-contract validation, and optional read-only anomaly detection without starting the full local pipeline
- `--run-profile-pipeline` adds a controlled profile-specific runtime E2E pass that starts only the required local services, uses bounded row/message limits, and verifies PostgreSQL row-count deltas after producer, consumer, and warehouse-loader execution
- `--concurrent-pipeline` is an opt-in Stage 23 runtime mode that now starts consumer, warehouse-loader, and producer through the local E2E helper in a minimal concurrent flow while preserving sequential mode as the default
- `--stream-output` switches manual runtime runs from captured/report mode to live terminal streaming so progress logs or `tqdm` can be seen directly

`--run-sample-pipeline` is still supported as a backward-compatible alias for `--profile sample --run-profile-pipeline`.

## What It Is

The local E2E smoke test is a safe repository-level validation helper that checks whether the project still looks runnable as one system without starting the full Kafka, PostgreSQL, Airflow, dbt, or AWS workflow by default.

Its goal is fast confidence, not full data processing. It validates the repository foundation, selected dataset profile, Docker Compose configuration, Python syntax, Terraform configuration, and optionally reuses the existing data-contract and anomaly-detection helpers in bounded, read-only-friendly ways.

The script lives at `scripts/run_local_e2e_smoke_test.py` and uses only the Python standard library.

For the final Stage 21 runbook, validated full-run summary, bottleneck notes, and PR-ready cleanup guidance, see [docs/stage-21-local-e2e-validation.md](docs/stage-21-local-e2e-validation.md).

For the final Stage 22 progress, batching, and full-benchmark runbook, see [docs/stage-22-progress-and-loader-optimization.md](docs/stage-22-progress-and-loader-optimization.md).

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
- `--concurrent-pipeline`
- `--stream-output`
- `--progress-mode auto|log|tqdm|bar`

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

Sequential mode remains the default Stage 23 behavior. If you add `--concurrent-pipeline`, the helper switches the JSON/report metadata to `pipeline_execution_mode=concurrent`, starts the Python consumer first, starts the warehouse loader second, waits briefly for warm-up, and starts the Go producer last.

Concurrent Stage 23 foundation example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --concurrent-pipeline --output-json docs/e2e-smoke-test-local.json
```

Current Stage 23D2 expectation:

- the command should parse and run safely
- the summary should report a real concurrent runtime result
- the JSON summary should record `pipeline_execution_mode` as `concurrent`
- the existing sequential path remains the default when `--concurrent-pipeline` is not used
- the concurrent runtime starts consumer first, warehouse loader second, then producer after a short warm-up
- the JSON summary now includes a dedicated `profile_pipeline_concurrent_runtime` section with per-process status, return code, duration, and orchestrator-termination details when concurrent mode is used

How progress is displayed in this mode:

- the Go producer prints interval-based progress lines in normal captured or `log` runs
- when you combine `--stream-output --progress-mode tqdm`, the E2E helper switches producer, consumer, and warehouse-loader to a cleaner custom `bar` mode
- that live `bar` mode uses larger in-place progress bars with carriage-return updates instead of repeated success progress lines
- the Python components still support `tqdm`, but the custom `bar` mode is preferred for live Docker and PowerShell streaming because it is easier to read
- when live bars are not active, the Python components fall back to regular interval-based log lines
- while the custom bar is active, successful per-batch `flushed warehouse batch ...` logs are also suppressed, while warnings, errors, and the final summary still remain visible
- the JSON summary records the effective per-component progress intervals under `profile_pipeline_progress`
- the summary now also records the effective producer progress mode and warehouse-loader `batch_size`

Why `tqdm` is hidden in captured mode:

- the local E2E helper normally captures subprocess stdout and stderr so it can write JSON excerpts
- once output is captured instead of attached to your terminal, `tqdm` no longer behaves like a live interactive progress bar
- captured mode is therefore still best for JSON-friendly debug logs, while live stream mode is now best for the larger custom progress bars

When you want live manual progress:

- add `--stream-output` so producer, consumer, and loader output is sent directly to your terminal
- prefer `--stream-output --progress-mode log` for live manual concurrent runs so three processes do not compete to redraw progress bars at the same time
- use `--progress-mode tqdm` if you want the helper to switch live stream mode into the cleaner custom bar display for all three pipeline stages
- use `--progress-mode bar` if you want to request that custom live progress-bar mode explicitly
- use `--progress-mode log` if you want plain interval-based logs even in a real terminal
- in those live bar-oriented modes, the E2E helper sets `PRODUCER_PROGRESS_MODE=bar` and `PYTHON_PROGRESS_MODE=bar`

Captured/report mode example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

Live manual progress mode example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --stream-output --progress-mode tqdm --output-json docs/e2e-smoke-test-local.json
```

Progress mode guidance:

- `auto` keeps the current safe default and behaves like `tqdm_if_tty_else_log`
- `log` is useful when you want stable line-based output in terminals, shell transcripts, or copy-paste-friendly reviews
- `tqdm` is useful as the friendly CLI trigger for the cleaner custom live bar mode in the local E2E helper
- `bar` is useful when you want to request that custom live progress-bar renderer explicitly

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
- `producer.mode`
- `consumer.progress_interval`
- `warehouse_loader.progress_interval`
- `warehouse_loader.batch_size`
- Python progress mode as `tqdm_if_tty_else_log`

Validated Stage 22C full `100000`-row result:

- available rows: `100000`
- expected rows: `100000`
- processed delta: `100000`
- invalid delta: `0`
- total delta: `100000`
- producer duration: `158.816s`
- consumer duration: `34.504s`
- warehouse loader duration: `15.279s`
- data contract validation duration: `1.589s`
- anomaly detection duration: `1.654s`
- `WAREHOUSE_LOADER_BATCH_SIZE`: `1000`
- progress intervals: producer `1000`, consumer `1000`, warehouse loader `1000`

Expected runtime note:

- a `100000`-row run can take noticeably longer than sample or medium validation
- Docker health, Kafka throughput, and local PostgreSQL performance will affect total runtime
- `PRODUCER_SEND_DELAY_MS=0` stays enforced for controlled runtime validation so the producer does not add artificial delay
- progress intervals default to `1000`, and the smoke-test helper may lower the interval for smaller bounded runs so at least one visible progress update is still emitted
- `WAREHOUSE_LOADER_BATCH_SIZE` defaults to `1000` for the controlled runtime flow so medium and full runs can compare loader timing more meaningfully
- Stage 22B adds batch insert optimization on top of the Stage 22A progress visibility foundation

Known before and after warehouse-loader comparison for the full `100000`-row path:

- Stage 21 baseline warehouse loader: `758.764s`
- Stage 22C post-optimization warehouse loader: `15.279s`
- correctness stayed intact with `processed_delta=100000` and `invalid_delta=0`

## Comparing Before And After Runtime

To compare warehouse-loader behavior before and after this optimization:

- run the same `medium` or `full` command with the same `--max-rows`
- compare `stage_durations_seconds.warehouse_loader` in the generated JSON summaries
- keep `WAREHOUSE_LOADER_BATCH_SIZE` fixed first, then vary it intentionally if you want to test different tradeoffs

Remember that the generated JSON summary is a local validation artifact only. `docs/e2e-smoke-test-local.json` is intentionally ignored by Git and should not be committed.

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
