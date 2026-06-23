# IoT Metadata Validator Lambda

This Lambda foundation validates lightweight S3 object metadata before future Step Functions orchestration begins. It does not call AWS APIs and uses only the Python standard library, so it can be smoke-tested locally without credentials.

## What it validates

- object key presence
- supported file extensions: `.csv`, `.json`, `.jsonl`, `.parquet`
- logical layer detection from key paths such as `raw/`, `processed/`, `curated/`, or `anomalies/`

## Supported event styles

- a standard S3-style event under `Records[0].s3`
- a simple direct test payload like `{"bucket": "iot-data-lake", "key": "raw/file.jsonl"}`

## Local smoke test

```powershell
python -m py_compile aws/lambda/iot_metadata_validator/handler.py
python aws/lambda/iot_metadata_validator/handler.py
```

## Example response fields

- `status`
- `is_valid`
- `bucket`
- `key`
- `detected_layer`
- `validation_errors`
- `metadata`
