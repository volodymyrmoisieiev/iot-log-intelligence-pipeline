# Stage 22 Pipeline Progress And Loader Optimization

## What Stage 22 Adds

Stage 22 improves long-running local pipeline validation in three connected steps:

- Stage 22A adds progress visibility for the Go producer, Python consumer, warehouse loader, and local E2E JSON summary
- Stage 22B adds `WAREHOUSE_LOADER_BATCH_SIZE` plus batched PostgreSQL inserts and batched Kafka offset commits on the warehouse-loader normal path
- Stage 22C validates the post-optimization `full` `100000`-row runtime flow and documents the measured before and after comparison

Together, these steps keep the pipeline behavior unchanged while making heavy local runs easier to monitor and much faster at the warehouse-loading stage.

## Full 100k Benchmark

Validated controlled full runtime command:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

Captured post-optimization result:

- available_rows: `100000`
- expected_rows: `100000`
- processed_delta: `100000`
- invalid_delta: `0`
- total_delta: `100000`
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

- processed delta matched the requested `100000` rows
- invalid delta remained `0`
- total delta matched the expected bounded row count

## Before And After Comparison

Known Stage 21 full baseline:

- full rows: `100000`
- producer: `153.289s`
- consumer: `56.767s`
- warehouse loader: `758.764s`
- processed_delta: `100000`
- invalid_delta: `0`

Stage 22C post-optimization full result:

- full rows: `100000`
- producer: `158.816s`
- consumer: `34.504s`
- warehouse loader: `15.279s`
- processed_delta: `100000`
- invalid_delta: `0`

Warehouse-loader comparison:

- baseline: `758.764s`
- post-optimization: `15.279s`
- absolute improvement: `743.485s`
- approximate speedup: `49.7x`

The main change behind that improvement is Stage 22B batching. The loader now writes rows and commits Kafka offsets in grouped flushes instead of paying that overhead per message on the normal path.

## Operational Notes

What Stage 22A visibility adds during longer runs:

- producer progress logs every `PRODUCER_PROGRESS_INTERVAL`
- consumer progress logs or `tqdm` progress when attached to a real terminal
- warehouse-loader progress logs or `tqdm` progress when attached to a real terminal
- local E2E JSON summary capture for effective progress intervals and loader batch size

What Stage 22B batching adds:

- `WAREHOUSE_LOADER_BATCH_SIZE` control with a safe default of `1000`
- batched inserts for `processed_iot_logs`
- batched inserts for `invalid_iot_logs`
- batched Kafka offset commits on the normal path
- per-message fallback if a batch insert fails, so error isolation remains safer

How to compare future runs:

- keep the same `--profile` and `--max-rows`
- keep `WAREHOUSE_LOADER_BATCH_SIZE` fixed when you want an apples-to-apples timing comparison
- compare `stage_durations_seconds.warehouse_loader` from the generated JSON summaries

## PR-Ready Cleanup Notes

Generated artifacts remain local-only and should not be committed:

- `docs/e2e-smoke-test-local.json`
- `docs/e2e-smoke-test-*.json`
- `data/raw/full_iot_logs.csv`
- `data/processed/medium_iot_logs.csv`
- ad hoc local runtime artifacts

The JSON report is intentionally ignored by Git and is meant for local inspection only.
