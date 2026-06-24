# Stage 21 Local E2E Validation

## What Stage 21 Adds

Stage 21 completes the repository's first end-to-end local validation workflow in five steps:

- Stage 21A adds a safe local E2E smoke-test foundation
- Stage 21B adds a controlled sample runtime E2E path
- Stage 21C adds a controlled medium runtime E2E path for `10000` rows
- Stage 21D adds a controlled full runtime E2E path for `100000` rows
- Stage 21E finalizes the documentation and PR-ready cleanup guidance

Together, these stages give the repository one reusable local validation entry point:

```text
scripts/run_local_e2e_smoke_test.py
```

That entry point now supports repository-level checks, bounded dataset validation, controlled runtime checks, JSON result capture, and explicit safeguards for heavier runs.

## Why Local E2E Validation Matters

Local E2E validation matters because this repository is not just a collection of isolated scripts. It is a system with moving parts:

- Docker Compose services
- Kafka topics and message flow
- Go producer behavior
- Python consumer validation
- warehouse loading into PostgreSQL
- data contract validation
- anomaly detection

Unit tests and syntax checks are still important, but they do not prove that these pieces work together under one controlled local run. Stage 21 fills that gap while still keeping the default workflow safe and intentional.

## Validation Modes

### Dry-run

Dry-run shows which smoke-test steps would execute and writes the plan into a JSON summary without running the external command steps.

Use it when you want the fastest sanity check or when you are preparing a larger runtime validation:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --dry-run --output-json docs/e2e-smoke-test-local.json
```

### Safe smoke test

The safe smoke test validates repository structure, resolved dataset path, Docker Compose config, Python syntax, Terraform validation, bounded data-contract validation, and optional read-only anomaly detection without starting the full local runtime flow.

Example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --output-json docs/e2e-smoke-test-local.json
```

### Sample runtime E2E

Sample runtime E2E starts the required local services and runs the producer, consumer, and warehouse loader on the tracked sample dataset with bounded limits.

Example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

`--run-sample-pipeline` remains available as a backward-compatible alias for this mode.

### Medium runtime E2E

Medium runtime E2E exercises the same controlled flow on a prepared `medium` dataset, typically `10000` rows.

Example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile medium --max-rows 10000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

If `data/processed/medium_iot_logs.csv` is missing, the preflight step fails clearly and points to the dataset preparation command.

### Full 100k runtime E2E

Full runtime E2E validates a bounded `100000`-row full-profile run and is the heaviest local Stage 21 path.

Example:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

## Why `--allow-full-run` Is Required

The full-profile path is intentionally gated by `--allow-full-run` because:

- it consumes noticeably more time and local resources than sample or medium validation
- it is easier to trigger by mistake if it looks like a routine default command
- it should remain an explicit opt-in workflow for intentional larger validation

Without `--allow-full-run`, the script refuses `--profile full --run-profile-pipeline` rather than attempting a heavy run by accident.

## How To Run Each Mode

### Dry-run

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --dry-run --output-json docs/e2e-smoke-test-local.json
```

### Safe smoke test

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --output-json docs/e2e-smoke-test-local.json
```

### Sample runtime E2E

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

### Medium runtime E2E

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile medium --max-rows 10000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

### Full 100k runtime E2E

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

## What Successful Full Validation Means

A successful full validation means all of the following are true:

- producer passed
- consumer passed
- warehouse loader passed
- PostgreSQL delta equals the expected bounded row count
- data contract validation passed
- anomaly detection passed, or reports a clear status and reason

For the validated Stage 21D example, success looked like this:

- full dataset rows: `100000`
- processed delta: `100000`
- invalid delta: `0`
- total delta: `100000`

## Example Full-run Summary

Example validated full-run metrics:

- full dataset rows: `100000`
- processed_delta: `100000`
- invalid_delta: `0`
- total_delta: `100000`
- producer duration: `153.289s`
- consumer duration: `56.767s`
- warehouse loader duration: `758.764s`
- data contract validation duration: `1.58s`
- anomaly detection duration: `1.227s`

These numbers are documented here for human review only. The generated JSON report itself is intentionally not committed.

## What Is Intentionally Not Committed

Stage 21 intentionally does not commit local heavyweight or generated validation artifacts such as:

- full dataset files
- medium dataset files
- local JSON smoke-test reports
- local runtime artifacts produced during ad hoc validation

In practice, that means items like these should stay out of commits:

- `data/raw/full_iot_logs.csv`
- `data/processed/medium_iot_logs.csv`
- `docs/e2e-smoke-test-local.json`
- `docs/e2e-smoke-test-*.json`

## Known Bottleneck

The warehouse loader is currently the slowest stage in the controlled runtime flow.

That was also visible in the validated `100000`-row full run, where warehouse loading took substantially longer than producer publishing or consumer validation.

## What To Do If The Full Dataset Is Missing

If `data/raw/full_iot_logs.csv` is missing, the full-profile preflight fails clearly before runtime execution starts.

To proceed:

1. Place the full dataset at `data/raw/full_iot_logs.csv`
2. Re-run the full command with `--allow-full-run`
3. Confirm the preflight reports the expected available row count before the runtime flow begins

If fewer rows are available than requested by `--max-rows`, either provide a larger full dataset or lower `--max-rows`.

## Future Improvements

Useful follow-up improvements after Stage 21 include:

- bulk inserts or PostgreSQL `COPY`-style optimization for the warehouse loader
- E2E report comparison between runs
- GitHub Actions artifact upload for selected validation summaries
- optional CI smoke test on a tiny sample profile
- performance regression tracking across repeated local runs

## Related Guides

- [docs/local-e2e-smoke-test.md](docs/local-e2e-smoke-test.md)
- [docs/dataset-profiles.md](docs/dataset-profiles.md)
