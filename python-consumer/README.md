# Python Consumer

This service consumes JSON messages from Kafka topic `iot_raw_logs`, validates and normalizes them, then publishes:

- valid records to `iot_processed_logs`
- invalid records to `iot_invalid_logs`

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers, default `kafka:9092`
- `KAFKA_RAW_TOPIC` - input topic, default `iot_raw_logs`
- `KAFKA_PROCESSED_TOPIC` - valid output topic, default `iot_processed_logs`
- `KAFKA_INVALID_TOPIC` - invalid output topic, default `iot_invalid_logs`
- `CONSUMER_GROUP_ID` - Kafka consumer group id, default `iot-python-consumer`
- `CONSUMER_MAX_MESSAGES` - max number of consumed messages before exit, default `0` for unlimited until idle timeout
- `CONSUMER_IDLE_TIMEOUT_SECONDS` - idle timeout before clean exit, default `10`
- `CONSUMER_PROGRESS_INTERVAL` - log progress every N consumed messages, default `1000`

## Safer medium and full runs

For `medium` and `full` dataset validation, use bounded runs so the consumer does not look stuck or run longer than intended.

- set a unique `CONSUMER_GROUP_ID` for each validation run so Kafka starts that run from a clean consumer-group state
- use `CONSUMER_MAX_MESSAGES` to stop after a known number of messages
- use `CONSUMER_PROGRESS_INTERVAL` to print regular progress updates during larger runs
- keep `full` dataset validation for manual/local or cloud-style checks rather than CI

Example PowerShell run:

```powershell
docker compose run --build --rm `
  -e CONSUMER_GROUP_ID=stage15d-consumer `
  -e CONSUMER_MAX_MESSAGES=10 `
  -e CONSUMER_PROGRESS_INTERVAL=5 `
  python-consumer
```

Runtime behavior:

- logs a startup line with brokers, topics, group id, max messages, idle timeout, and progress interval
- shows a `tqdm` progress bar when `tqdm` is installed and the process is attached to a real terminal
- falls back to regular interval-based logs when `tqdm` is unavailable or output is being captured
- logs progress every `CONSUMER_PROGRESS_INTERVAL` consumed messages
- exits clearly if no messages arrive before the idle timeout
- prints a final summary with `consumed`, `processed`, `invalid`, `failed`, `max_messages`, and `group_id`

## Validation rules

Required fields:

- `event_timestamp`
- `device_id`
- `source_ip`
- `destination_ip`
- `protocol`
- `packet_size`
- `duration_ms`
- `event_type`
- `status`

Additional checks:

- payload must be valid JSON object
- `event_timestamp` must be a valid ISO-8601 timestamp
- `protocol` must be `TCP`, `UDP`, or `ICMP`
- `packet_size` must be a non-negative integer
- `duration_ms` must be a non-negative integer
- `event_type` must be `normal`, `warning`, `error`, or `attack`

## Run tests

```powershell
Set-Location .\python-consumer
python -m pytest
```
