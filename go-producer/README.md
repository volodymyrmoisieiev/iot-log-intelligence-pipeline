# Go Producer

This service reads an IoT CSV file, converts each row into a JSON message, adds `ingestion_timestamp`, and publishes the result to Kafka topic `iot_raw_logs`.

By default it still uses the tracked sample dataset:

`data/samples/sample_iot_logs.csv`

For the final Stage 22 progress, batching, benchmark, and cleanup runbook, see [docs/stage-22-progress-and-loader-optimization.md](../docs/stage-22-progress-and-loader-optimization.md).

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers, for example `kafka:9092` in Docker or `localhost:29092` locally
- `KAFKA_RAW_TOPIC` - target Kafka topic, default `iot_raw_logs`
- `DATASET_PROFILE` - dataset mode, allowed values `sample`, `medium`, `full`, default `sample`
- `PRODUCER_INPUT_FILE` - optional explicit CSV input path override; if set, it overrides `DATASET_PROFILE`
- `PRODUCER_MAX_ROWS` - optional positive row limit; `0` means no limit
- `PRODUCER_PROGRESS_INTERVAL` - print producer progress every N attempted rows, default `1000`
- `PRODUCER_PROGRESS_MODE` - successful progress output style, `log` or `bar`, default `log`
- `PRODUCER_SEND_DELAY_MS` - delay between messages in milliseconds, default `250`

## Dataset profile paths

- `sample` -> `/app/data/samples/sample_iot_logs.csv`
- `medium` -> `/app/data/processed/medium_iot_logs.csv`
- `full` -> `/app/data/raw/full_iot_logs.csv`

If `DATASET_PROFILE=medium` and the generated file is missing, the producer exits with a clear error that tells you to create it first with `scripts/create_dataset_profile.py`.

## Run with Docker Compose

Default sample run:

```powershell
docker compose up -d kafka kafka-ui kafka-init
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 go-producer
```

Sample profile with a row cap:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=sample -e PRODUCER_SEND_DELAY_MS=0 -e PRODUCER_MAX_ROWS=5 go-producer
```

Medium profile after generating `data/processed/medium_iot_logs.csv`:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=medium -e PRODUCER_SEND_DELAY_MS=0 -e PRODUCER_MAX_ROWS=5 go-producer
```

Full profile warning:

```powershell
docker compose run --build --rm -e DATASET_PROFILE=full -e PRODUCER_SEND_DELAY_MS=0 go-producer
```

Use `full` only when `data/raw/full_iot_logs.csv` exists locally and you intentionally want a heavier manual run.

Runtime behavior:

- prints startup settings including the resolved progress interval
- prints progress lines every `PRODUCER_PROGRESS_INTERVAL` attempted rows in `log` mode
- can switch to `PRODUCER_PROGRESS_MODE=bar` for a one-line updating progress display during live manual runs
- keeps the final sent/failed/skipped summary for bounded and larger runs

Live manual progress-bar style example:

```powershell
docker compose run --build --rm -e PRODUCER_SEND_DELAY_MS=0 -e PRODUCER_PROGRESS_MODE=bar go-producer
```

## Run locally with Go

If Go is installed locally:

```powershell
Set-Location .\go-producer
go run .
```
