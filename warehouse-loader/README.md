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

## Loader behavior

- consumes from both `iot_processed_logs` and `iot_invalid_logs`
- inserts valid records into `processed_iot_logs`
- inserts invalid records into `invalid_iot_logs`
- preserves the full processed JSON message in PostgreSQL column `raw_payload`
- supports bounded runs with `WAREHOUSE_LOADER_MAX_MESSAGES`
- exits cleanly after `WAREHOUSE_LOADER_IDLE_TIMEOUT_SECONDS` of inactivity

## Run tests

```bash
cd warehouse-loader
python -m pytest
```
