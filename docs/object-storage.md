# Object Storage

## Stage 11A scope

Stage 11A adds a local S3-compatible object storage foundation through MinIO.

This stage is intentionally limited to local development infrastructure:

- it uses MinIO, not production AWS S3
- it creates one local bucket: `iot-data-lake`
- it does not upload Spark output yet
- it does not integrate MinIO into Airflow yet
- it does not add AWS, EMR, Glue, Terraform, Kubernetes, deployment, or secrets

## Local endpoints

- S3 API: [http://localhost:9000](http://localhost:9000/)
- MinIO console: [http://localhost:9001](http://localhost:9001/)

## Local credentials

- username: `minioadmin`
- password: `minioadmin`

## Local environment variables

The repository `.env.example` includes these local placeholders:

```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=iot-data-lake
```

`MINIO_ENDPOINT` uses the Docker Compose service hostname so helper containers can reach MinIO inside the Compose network.

## Services added

- `minio`
  - image: `minio/minio`
  - container name: `iot-minio`
  - ports: `9000:9000` and `9001:9001`
  - command: `server /data --console-address ":9001"`
- `minio-init`
  - image: `minio/mc`
  - waits for MinIO to become reachable
  - creates bucket `iot-data-lake`
  - uses idempotent bucket creation so repeated runs stay safe

Both services use the Docker Compose profile `object-storage`, which keeps the existing default pipeline startup unchanged unless you explicitly opt in.

## How to start MinIO

Validate the Compose file:

```bash
docker compose config
```

Start only the object-storage services:

```bash
docker compose --profile object-storage up -d minio minio-init
```

Check container state:

```bash
docker compose ps
```

## How to verify the bucket

Option 1: MinIO console

- open [http://localhost:9001](http://localhost:9001/)
- sign in with `minioadmin / minioadmin`
- confirm bucket `iot-data-lake` exists

Option 2: rerun the idempotent init container

```bash
docker compose --profile object-storage run --rm minio-init
```

This safely re-applies bucket creation and lists the bucket path, which confirms the bucket still exists.

## Notes

- MinIO data is persisted in the named Docker volume `minio_data`
- repeated `minio-init` runs should not fail if the bucket already exists
- this stage does not modify the Go producer, Python consumer, warehouse loader, dbt models, Streamlit dashboard, Spark job logic, Airflow DAG logic, or GitHub Actions CI
