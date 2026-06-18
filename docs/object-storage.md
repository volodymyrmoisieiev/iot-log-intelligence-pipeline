# Object Storage

## Stage 11A, 11B, and 11C scope

Stage 11A adds a local S3-compatible object storage foundation through MinIO.

Stage 11B builds on that foundation and adds a local uploader for Spark device-feature Parquet output.

Stage 11C wires that uploader into the existing local Airflow DAG so Airflow can start MinIO, upload Spark Parquet output, and validate uploaded objects.

These stages are intentionally limited to local development infrastructure:

- they use MinIO, not production AWS S3
- they use one local bucket: `iot-data-lake`
- they upload Spark output from `data/processed/spark/device_features`
- they store uploaded files under `spark/device_features/latest/`
- Airflow can now upload and validate objects in local MinIO
- they do not add AWS, EMR, Glue, Terraform, Kubernetes, deployment, or secrets

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
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=iot-data-lake
SPARK_FEATURES_LOCAL_PATH=data/processed/spark/device_features
SPARK_FEATURES_CONTAINER_PATH=/workspace/data/processed/spark/device_features
SPARK_FEATURES_OBJECT_PREFIX=spark/device_features/latest
```

`MINIO_ENDPOINT` uses the Docker Compose service hostname for helper containers inside the Compose network.

## Services and files added

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
- `object-storage-uploader`
  - builds from `object-storage/Dockerfile`
  - runs `object-storage/upload_spark_features.py`
  - uploads `.parquet` files to `iot-data-lake`
  - uses object prefix `spark/device_features/latest/`

Both services use the Docker Compose profile `object-storage`, which keeps the existing default pipeline startup unchanged unless you explicitly opt in.

Airflow integration in Stage 11C reuses the same services through the existing local `iot_local_pipeline_dag`.

## Upload script behavior

The uploader script:

- reads Parquet files from `data/processed/spark/device_features`
- fails clearly if the folder does not exist
- fails clearly if no `.parquet` files are found
- connects to MinIO through the S3-compatible API using `boto3`
- checks that bucket `iot-data-lake` exists before upload
- uploads files under `spark/device_features/latest/`
- prints each uploaded object name and the final upload count

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

## How to generate Spark output

If the local Parquet output is missing or you want fresh output, run:

```bash
docker compose run --build --rm spark-batch python /app/jobs/device_features_job.py
```

Expected local input path:

- `data/processed/spark/device_features`

## How to upload Spark output to MinIO

Run the uploader service:

```bash
docker compose --profile object-storage run --build --rm object-storage-uploader
```

Successful uploads target:

- bucket: `iot-data-lake`
- prefix: `spark/device_features/latest/`

## Airflow integration

Stage 11C adds these `iot_local_pipeline_dag` tasks after local Spark output validation:

- `start_object_storage`
- `upload_spark_features_to_minio`
- `validate_minio_spark_features_upload`

Updated Airflow sequence:

- local Spark output is generated in `data/processed/spark/device_features`
- Airflow validates local Parquet output exists
- Airflow starts local MinIO services under the `object-storage` profile
- Airflow uploads Parquet files into bucket `iot-data-lake`
- Airflow validates that at least one `.parquet` object exists under `spark/device_features/latest/`

This remains local MinIO only and does not add production AWS S3.

## How to verify uploaded objects

Option 1: MinIO console

- open [http://localhost:9001](http://localhost:9001/)
- sign in with `minioadmin / minioadmin`
- open bucket `iot-data-lake`
- confirm objects appear under `spark/device_features/latest/`

Option 2: uploader logs

- review the printed `Uploaded: s3://...` lines
- confirm the final uploaded file count

Option 3: rerun the uploader safely

- rerunning the uploader overwrites the same object keys in MinIO and should succeed as long as the source Parquet files still exist

## Notes

- MinIO data is persisted in the named Docker volume `minio_data`
- repeated `minio-init` runs should not fail if the bucket already exists
- Stage 11C adds Airflow integration for local MinIO upload and validation
- this is local MinIO only, not real AWS S3
