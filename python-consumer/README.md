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

```bash
cd python-consumer
python -m pytest
```
