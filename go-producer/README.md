# Go Producer

This service reads the sample IoT CSV file, converts each row into a JSON message, adds `ingestion_timestamp`, and publishes the result to Kafka topic `iot_raw_logs`.

## Input file

Default input file:

`data/samples/sample_iot_logs.csv`

Inside the container, the same file is mounted at:

`/app/data/samples/sample_iot_logs.csv`

## Environment variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers, for example `kafka:9092` in Docker or `localhost:29092` locally
- `KAFKA_RAW_TOPIC` - target Kafka topic, default `iot_raw_logs`
- `PRODUCER_INPUT_FILE` - CSV input path, default `/app/data/samples/sample_iot_logs.csv`
- `PRODUCER_SEND_DELAY_MS` - delay between messages in milliseconds, default `250`

## Run with Docker Compose

```bash
docker compose up -d kafka kafka-ui kafka-init
docker compose run --rm go-producer
```

## Run locally with Go

If Go is installed locally:

```bash
cd go-producer
go run .
```
