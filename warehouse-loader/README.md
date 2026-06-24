# Warehouse Loader

This service consumes validated Kafka messages from `iot_processed_logs` and invalid messages from `iot_invalid_logs`, then loads them into PostgreSQL tables `processed_iot_logs` and `invalid_iot_logs`.

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers, default `kafka:9092`
- `KAFKA_PROCESSED_TOPIC` - processed input topic, default `iot_processed_logs`
- `KAFKA_INVALID_TOPIC` - invalid input topic, default `iot_invalid_logs`
- `POSTGRES_DB` - PostgreSQL database name, default `iot_logs`
- `POSTGRES_USER` - PostgreSQL user, default `iot_user`
- `POSTGRES_PASSWORD` - PostgreSQL password, default `iot_password`
- `POSTGRES_HOST` - PostgreSQL host, default `postgres`
- `POSTGRES_PORT` - PostgreSQL port, default `5432`
- `WAREHOUSE_LOADER_GROUP_ID` - Kafka consumer group id, default `iot-warehouse-loader`
- `WAREHOUSE_LOADER_MAX_MESSAGES` - max number of consumed messages before exit, default `0` for unlimited until idle timeout
- `WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS` - idle timeout before clean exit, default `10`
- `WAREHOUSE_LOADER_PROGRESS_INTERVAL` - log progress every N consumed messages, default `1000`
- `WAREHOUSE_LOADER_BATCH_SIZE` - insert and Kafka-offset commit batch size, default `1000`

## Loader behavior

- consumes from both `iot_processed_logs` and `iot_invalid_logs`
- inserts valid records into `processed_iot_logs`
- inserts invalid records into `invalid_iot_logs`
- preserves the full processed JSON message in PostgreSQL column `raw_payload`
- supports bounded runs with `WAREHOUSE_LOADER_MAX_MESSAGES`
- shows a `tqdm` progress bar when `tqdm` is installed and the process is attached to a real terminal
- falls back to regular interval-based logs when `tqdm` is unavailable or output is being captured
- batches PostgreSQL inserts and Kafka offset commits with `WAREHOUSE_LOADER_BATCH_SIZE`
- logs regular progress updates during larger runs with `WAREHOUSE_LOADER_PROGRESS_INTERVAL`
- exits cleanly after `WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS` of inactivity

## Batch insert optimization

Stage 22B changes the loader's normal path from per-row PostgreSQL inserts plus per-message offset commits to batched inserts followed by batched offset commits.

What that means:

- rows are buffered in memory until `WAREHOUSE_LOADER_BATCH_SIZE` is reached or the run is ending
- processed and invalid rows are still written to the same tables with the same schema
- Kafka topics and message payloads stay unchanged
- the startup log now includes `batch_size`
- flush logs show how many buffered rows were written in each batch

Why this helps:

- large runs such as `medium`, `full`, or `100000` rows spend much less time paying per-row PostgreSQL round-trip overhead
- Kafka offset commits happen once per flushed batch instead of once per message on the normal path
- the runtime can still fall back to per-message handling if a batch flush fails, which keeps failure isolation safer

## Safer medium and full runs

For `medium` and `full` dataset validation, prefer bounded runs and a unique loader consumer group id.

- set a unique `WAREHOUSE_LOADER_GROUP_ID` so the loader consumes with a clean validation group
- use `WAREHOUSE_LOADER_MAX_MESSAGES` to limit the run size
- use `WAREHOUSE_LOADER_PROGRESS_INTERVAL` to print regular progress updates
- tune `WAREHOUSE_LOADER_BATCH_SIZE` to compare throughput before and after optimization
- keep `full` dataset validation for manual/local or cloud-style checks rather than CI

Example PowerShell run:

```powershell
docker compose run --build --rm `
  -e WAREHOUSE_LOADER_GROUP_ID=stage15d-loader `
  -e WAREHOUSE_LOADER_MAX_MESSAGES=10 `
  -e WAREHOUSE_LOADER_PROGRESS_INTERVAL=5 `
  -e WAREHOUSE_LOADER_BATCH_SIZE=10 `
  warehouse-loader
```

Final runtime summary includes `consumed`, `inserted_processed`, `inserted_invalid`, `failed`, `batches_flushed`, `batch_size`, `max_messages`, and `group_id`.

## Run tests

```powershell
Set-Location .\warehouse-loader
python -m pytest
```
