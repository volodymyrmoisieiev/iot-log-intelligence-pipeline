# Stage 22 Progress And Loader Optimization

## What Stage 22 Adds

Stage 22 finishes the repository's first focused usability-and-performance pass for long-running local pipeline validation:

- Stage 22A adds progress visibility for the Go producer, Python consumer, warehouse loader, and local E2E JSON summary
- Stage 22B adds `WAREHOUSE_LOADER_BATCH_SIZE` plus batched PostgreSQL inserts and batched Kafka offset commits on the warehouse-loader normal path
- Stage 22C validates the optimized `full` `100000`-row runtime flow and captures the measured before and after benchmark
- Stage 22D finalizes the runbook, links, validation notes, and PR-ready cleanup guidance

Together, these steps keep pipeline business logic unchanged while making heavy local runs easier to monitor and dramatically faster at the warehouse-loading stage.

## Why Progress Visibility Matters

Before Stage 22A, longer `medium`, `full`, or `100000`-row validation runs could look idle even when they were still making progress. That creates two problems:

- it is harder to tell whether the pipeline is healthy or stuck
- it is harder to understand which stage is actually taking the time

Stage 22A fixes that by adding visible progress signals across producer, consumer, loader, and the local E2E summary. That visibility also made the warehouse-loader bottleneck much easier to confirm before Stage 22B optimization work.

One nuance from Stage 22A is that the local E2E helper normally captures subprocess stdout and stderr so it can keep JSON excerpts. That preserved machine-readable reporting, but it also meant `tqdm` was not visible as a live terminal progress bar during manual E2E runs. The Stage 22 live-output follow-up adds an explicit manual `--stream-output` mode for those cases.

## How Progress Is Shown

### Go producer

- the producer logs startup settings, including the resolved progress interval
- it prints progress lines every `PRODUCER_PROGRESS_INTERVAL` attempted rows in normal `log` mode
- it also supports `PRODUCER_PROGRESS_MODE=bar` for cleaner live manual runs, using a one-line updating progress display instead of repeated progress lines
- each progress line shows attempted, total, sent, and failed counts

### Python consumer

- the consumer logs startup settings, including the progress interval
- when `tqdm` is installed and the process is attached to a real terminal, it shows a live progress bar
- while that bar is active, repetitive success progress INFO lines are suppressed
- when `tqdm` is unavailable or output is being captured, it falls back to regular interval-based log lines
- progress updates happen every `CONSUMER_PROGRESS_INTERVAL` consumed messages

### Warehouse loader

- the loader logs startup settings, including progress interval and batch size
- when `tqdm` is installed and the process is attached to a real terminal, it shows a live progress bar
- while that bar is active, repetitive interval progress INFO lines and successful `flushed warehouse batch ...` INFO lines are suppressed
- when `tqdm` is unavailable or output is being captured, it falls back to regular interval-based log lines
- progress updates happen every `WAREHOUSE_LOADER_PROGRESS_INTERVAL` consumed messages
- flush logs show how many buffered rows were written in each batch
- final summary logs include inserted totals, failures, flushed batches, and batch size

### Local E2E JSON summary

The local E2E helper records runtime stage durations plus the effective progress configuration under `profile_pipeline_progress`, including:

- producer progress interval
- producer progress mode
- consumer progress interval
- warehouse-loader progress interval
- warehouse-loader batch size
- Python progress mode as `tqdm_if_tty_else_log`

Stage 22 also supports two manual-observation controls in the local E2E helper:

- `--stream-output` streams subprocess stdout and stderr directly to the terminal instead of fully capturing them
- `--progress-mode auto|log|tqdm` controls how the Python consumer and warehouse loader choose between progress bars and interval logs
- when that combination is `--stream-output --progress-mode tqdm`, the helper also sets `PRODUCER_PROGRESS_MODE=bar` so the Go producer switches to a cleaner progress-bar-first display

## What `tqdm` Does

`tqdm` is an optional terminal progress-bar library used only by the Python consumer and warehouse loader.

What it improves:

- clearer live visual feedback during longer runs
- immediate sense of throughput while the process is attached to a real terminal

Fallback behavior:

- if `tqdm` is not installed, the components still work
- if output is being captured instead of shown on a terminal, the components fall back to plain interval-based log lines
- CI-friendly and `py_compile`-safe behavior is preserved because the import is optional and handled safely

Mode behavior:

- `auto` means use `tqdm` only when output is attached to a real terminal, otherwise use logs
- `log` means always use interval-based logging
- `tqdm` means try to render `tqdm` even when normal TTY detection would have chosen logs, with safe fallback to logs if `tqdm` is unavailable

## Progress And Batch Controls

### `PRODUCER_PROGRESS_INTERVAL`

- controls how often the Go producer prints progress lines
- lower values show progress more often
- higher values reduce log volume during larger runs

### `PRODUCER_PROGRESS_MODE`

- controls how the Go producer renders successful progress output
- `log` keeps the existing repeated interval-based progress lines
- `bar` switches to a one-line updating progress display that is easier to read during live manual E2E runs
- default: `log`

### `CONSUMER_PROGRESS_INTERVAL`

- controls how often the Python consumer reports consumed-message progress
- affects both regular log mode and `tqdm`-backed runs

### `WAREHOUSE_LOADER_PROGRESS_INTERVAL`

- controls how often the warehouse loader reports consumed-message progress
- helps confirm that large loads are still moving forward

### `WAREHOUSE_LOADER_BATCH_SIZE`

- controls how many buffered records the warehouse loader writes in one batch on the normal path
- the same setting also determines how often Kafka offsets are committed in batches on that path
- default: `1000`

### Manual live-output controls

- `--stream-output` is useful for manual testing when you want to watch progress live instead of relying on captured JSON excerpts
- `--progress-mode log` is useful when you want predictable line-based output
- `--progress-mode tqdm` is useful when you want the clearest live terminal feedback during manual E2E runs, with the Python components suppressing repetitive success logs and the Go producer switching to `PRODUCER_PROGRESS_MODE=bar`
- when `--stream-output` is not used, the original captured/report behavior stays unchanged

## Why Batching Improved Performance

Before Stage 22B, the warehouse loader paid PostgreSQL insert overhead and Kafka offset-commit overhead per message. That made the loader far slower than the producer and consumer on large runs.

Stage 22B changes the normal path to:

- buffer rows in memory
- insert them into PostgreSQL in grouped batches
- commit Kafka offsets in grouped batches after a successful flush

This reduces round trips and per-message transaction overhead dramatically while keeping:

- Kafka topics unchanged
- message schema unchanged
- PostgreSQL schema unchanged
- correctness checks unchanged

If a batch insert fails, the loader can still fall back to per-message handling so failure isolation remains safer than a hard-stop batch-only design.

## Full 100k Benchmark

Validated controlled full runtime command:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

Captured post-optimization result:

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
- producer progress interval: `1000`
- consumer progress interval: `1000`
- warehouse-loader progress interval: `1000`

Correctness result:

- `processed_delta=100000`
- `invalid_delta=0`
- `total_delta=100000`

## Before And After Benchmark

Known Stage 21 baseline:

- warehouse loader: `758.764s`

Measured Stage 22C optimized result:

- warehouse loader: `15.279s`

Benchmark summary:

- before: `758.764s`
- after: `15.279s`
- speedup: `49.7x`
- absolute improvement: `743.485s`

The most important point is that this improvement came without changing the runtime data contract, message routing, or warehouse schema, and correctness remained intact at `100000` processed rows with `0` invalid rows.

## How To Compare Future Runs

- keep the same `--profile` and `--max-rows`
- keep `WAREHOUSE_LOADER_BATCH_SIZE` fixed when you want an apples-to-apples timing comparison
- compare `stage_durations_seconds.warehouse_loader` from the generated JSON summaries
- check `profile_pipeline_progress` to confirm the effective batch size and progress intervals used during that run

## PR-Ready Cleanup Notes

Generated artifacts remain local-only and should not be committed:

- `docs/e2e-smoke-test-local.json`
- `docs/e2e-smoke-test-*.json`
- `data/raw/full_iot_logs.csv`
- `data/processed/medium_iot_logs.csv`
- `.terraform/`
- `.terraform.lock.hcl`
- ad hoc runtime logs and other local runtime artifacts

The JSON report is intentionally ignored by Git and is meant for local inspection only.

Examples:

Captured/report mode:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

Live manual progress mode:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --stream-output --progress-mode tqdm --output-json docs/e2e-smoke-test-local.json
```

## Limitations And Future Improvements

Useful follow-up work after Stage 22 includes:

- PostgreSQL `COPY`-style loading for even larger throughput gains
- larger-batch tuning to compare tradeoffs in memory use, lock duration, and throughput
- CI-side performance regression checks for selected bounded runs
- local E2E report comparison tooling across repeated benchmark runs
