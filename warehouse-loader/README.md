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

## Loader behavior

- consumes from both `iot_processed_logs` and `iot_invalid_logs`
- inserts valid records into `processed_iot_logs`
- inserts invalid records into `invalid_iot_logs`
- preserves the full processed JSON message in PostgreSQL column `raw_payload`
- supports bounded runs with `WAREHOUSE_LOADER_MAX_MESSAGES`
- shows a `tqdm` progress bar when `tqdm` is installed and the process is attached to a real terminal
- falls back to regular interval-based logs when `tqdm` is unavailable or output is being captured
- logs regular progress updates during larger runs with `WAREHOUSE_LOADER_PROGRESS_INTERVAL`
- exits cleanly after `WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS` of inactivity

## Safer medium and full runs

For `medium` and `full` dataset validation, prefer bounded runs and a unique loader consumer group id.

- set a unique `WAREHOUSE_LOADER_GROUP_ID` so the loader consumes with a clean validation group
- use `WAREHOUSE_LOADER_MAX_MESSAGES` to limit the run size
- use `WAREHOUSE_LOADER_PROGRESS_INTERVAL` to print regular progress updates
- keep `full` dataset validation for manual/local or cloud-style checks rather than CI

Example PowerShell run:

```powershell
docker compose run --build --rm `
  -e WAREHOUSE_LOADER_GROUP_ID=stage15d-loader `
  -e WAREHOUSE_LOADER_MAX_MESSAGES=10 `
  -e WAREHOUSE_LOADER_PROGRESS_INTERVAL=5 `
  warehouse-loader
```

Final runtime summary includes `consumed`, `inserted_processed`, `inserted_invalid`, `failed`, `max_messages`, and `group_id`.

## Run tests

```powershell
Set-Location .\warehouse-loader
python -m pytest
```
