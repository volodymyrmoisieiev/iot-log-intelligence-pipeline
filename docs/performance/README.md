# Performance / Load Testing

## Overview

Stage 16 starts the repository's local performance and load-testing foundation.

Stage 16A does not change the Go producer, Python consumer, warehouse loader, Airflow DAG logic, dbt models, Spark jobs, MinIO logic, or Terraform logic. It adds a lightweight benchmark helper so you can measure how long the local Docker-based pipeline takes across the existing `sample`, `medium`, and `full` dataset profiles.

The current benchmark scope is intentionally narrow:

- Go producer timing
- Python consumer timing
- warehouse loader timing
- JSON result capture for local comparison
- optional Markdown summary generation for human-readable reporting

The helper script lives at `scripts/run_performance_benchmark.py`.

## Before You Run a Benchmark

Start the local dependencies that the benchmarked services expect:

```powershell
docker compose config
docker compose up -d kafka kafka-init postgres
```

The benchmark helper assumes those shared services are already available. That keeps benchmark results focused on producer, consumer, and loader runtime rather than one-time container startup cost.

## Run a Sample Benchmark

Run a quick sample-profile benchmark with the default Stage 15-safe bounds:

```powershell
python .\scripts\run_performance_benchmark.py --profile sample --rows 72
```

Use `--dry-run` when you want to confirm commands and output paths without executing Docker Compose:

```powershell
python .\scripts\run_performance_benchmark.py --dry-run --profile sample --rows 72
```

Generate a local Markdown summary alongside the JSON result:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_performance_benchmark.py --profile sample --rows 72 --summary-md docs/performance/benchmark-summary-local.md
```

That `benchmark-summary-local.md` file is a local artifact unless you intentionally decide to commit a specific result for portfolio evidence.

## Generate a Medium Dataset

The `medium` dataset is a local generated artifact and is not committed to git by default.

Create it from the larger raw source CSV:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

If `data/processed/medium_iot_logs.csv` is missing, the benchmark helper exits early and prints this generation command for you.

## Run a Medium Benchmark

After the medium dataset exists, run a bounded medium benchmark such as:

```powershell
python .\scripts\run_performance_benchmark.py --profile medium --rows 1000
```

If you want separate limits for later pipeline steps, override them explicitly:

```powershell
python .\scripts\run_performance_benchmark.py --profile medium --rows 1000 --consumer-messages 1000 --loader-messages 1000
```

## Result Files

Benchmark JSON reports are written under `docs/performance/results/` by default.

Each report captures:

- benchmark timestamp
- selected dataset profile
- row and message caps
- command names
- per-step elapsed seconds
- total elapsed seconds
- return codes

When `--summary-md` is provided, the script also writes a human-readable Markdown summary that includes:

- benchmark timestamp
- dataset profile
- rows and message caps
- elapsed time per step
- total elapsed time
- return codes
- a short interpretation section
- a note that the numbers depend on the local machine and Docker resources

These result files are local artifacts and should stay out of normal commits unless you intentionally choose one as portfolio evidence.

Use the result files this way:

- JSON result file: machine-readable benchmark artifact stored under `docs/performance/results/`
- Markdown summary file: optional human-readable local summary such as `docs/performance/benchmark-summary-local.md`
- committed example summary: [docs/performance/benchmark-summary-example.md](benchmark-summary-example.md), which shows how to read the generated output without committing local run artifacts

Git is configured to ignore generated JSON benchmark results and the local `benchmark-summary-local.md` file while still allowing the folder structure and the committed example summary to remain in the repository.
