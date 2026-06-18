from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


REPO_ROOT = Path(__file__).resolve().parents[1]


def get_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value or default


def resolve_local_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def collect_parquet_files(local_path: Path) -> list[Path]:
    if not local_path.exists():
        raise FileNotFoundError(
            f"Spark features path does not exist: {local_path}"
        )

    if not local_path.is_dir():
        raise NotADirectoryError(
            f"Spark features path is not a directory: {local_path}"
        )

    parquet_files = sorted(
        file_path
        for file_path in local_path.rglob("*.parquet")
        if file_path.is_file()
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"No .parquet files found under: {local_path}"
        )

    return parquet_files


def build_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def check_bucket_exists(s3_client, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as exc:
        raise RuntimeError(
            f"Unable to access bucket '{bucket_name}'. "
            "Ensure MinIO is running and the bucket exists."
        ) from exc


def upload_files(
    s3_client,
    bucket_name: str,
    prefix: str,
    local_root: Path,
    parquet_files: list[Path],
) -> list[str]:
    uploaded_objects: list[str] = []
    normalized_prefix = prefix.strip("/")

    for file_path in parquet_files:
        relative_path = file_path.relative_to(local_root).as_posix()
        object_key = f"{normalized_prefix}/{relative_path}"
        s3_client.upload_file(str(file_path), bucket_name, object_key)
        uploaded_objects.append(object_key)
        print(f"Uploaded: s3://{bucket_name}/{object_key}")

    return uploaded_objects


def main() -> int:
    endpoint = get_env("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = get_env("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = get_env("MINIO_SECRET_KEY", "minioadmin")
    bucket_name = get_env("MINIO_BUCKET", "iot-data-lake")
    local_path = resolve_local_path(
        get_env(
            "SPARK_FEATURES_LOCAL_PATH",
            "data/processed/spark/device_features",
        )
    )
    object_prefix = get_env(
        "SPARK_FEATURES_OBJECT_PREFIX",
        "spark/device_features/latest",
    )

    try:
        parquet_files = collect_parquet_files(local_path)
        s3_client = build_s3_client(endpoint, access_key, secret_key)
        check_bucket_exists(s3_client, bucket_name)
        uploaded_objects = upload_files(
            s3_client,
            bucket_name,
            object_prefix,
            local_path,
            parquet_files,
        )
    except (BotoCoreError, ClientError, OSError, RuntimeError) as exc:
        print(f"Upload failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Upload complete: {len(uploaded_objects)} file(s) uploaded "
        f"to s3://{bucket_name}/{object_prefix.strip('/')}/"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
