import json
import os
from pathlib import Path
from urllib.parse import unquote_plus

SUPPORTED_EXTENSIONS = {".csv", ".json", ".jsonl", ".parquet"}
SUPPORTED_LAYERS = ("raw", "processed", "curated", "anomalies")


def detect_layer_from_key(key):
    normalized_parts = [part.strip().lower() for part in key.split("/") if part.strip()]
    for part in normalized_parts:
        if part in SUPPORTED_LAYERS:
            return part
    return None


def extract_event_details(event):
    if not isinstance(event, dict):
        return None, None, {"source_type": "unsupported_event"}

    records = event.get("Records")
    if isinstance(records, list) and records:
        first_record = records[0] or {}
        s3_payload = first_record.get("s3") or {}
        bucket = (s3_payload.get("bucket") or {}).get("name")
        key = (s3_payload.get("object") or {}).get("key")
        decoded_key = unquote_plus(str(key)) if key else None

        return bucket, decoded_key, {
            "source_type": "s3_event",
            "event_name": first_record.get("eventName"),
            "record_count": len(records),
        }

    bucket = event.get("bucket") or event.get("s3_bucket")
    key = event.get("key") or event.get("object_key") or event.get("s3_key")
    decoded_key = unquote_plus(str(key)) if key else None

    return bucket, decoded_key, {
        "source_type": "direct_event",
        "record_count": 1,
    }


def validate_metadata(bucket, key, base_metadata):
    validation_errors = []
    detected_layer = None
    extension = None

    if not key:
        validation_errors.append("Missing object key in event payload.")
    else:
        extension = os.path.splitext(key.lower())[1]
        if extension not in SUPPORTED_EXTENSIONS:
            validation_errors.append(
                "Unsupported file extension. Supported extensions: .csv, .json, .jsonl, .parquet."
            )
        detected_layer = detect_layer_from_key(key)

    metadata = dict(base_metadata)
    metadata.update(
        {
            "file_extension": extension,
            "filename": os.path.basename(key) if key else None,
            "key_directory": os.path.dirname(key) if key else None,
            "supported_extensions": sorted(SUPPORTED_EXTENSIONS),
        }
    )

    is_valid = not validation_errors
    status = "validated" if is_valid else "invalid"

    response = {
        "status": status,
        "is_valid": is_valid,
        "bucket": bucket,
        "key": key,
        "detected_layer": detected_layer,
        "validation_errors": validation_errors,
        "metadata": metadata,
    }

    print(
        json.dumps(
            {
                "message": "Lambda metadata validator completed.",
                "status": status,
                "bucket": bucket,
                "key": key,
                "detected_layer": detected_layer,
                "validation_errors": validation_errors,
            }
        )
    )
    return response


def lambda_handler(event, context):
    del context

    bucket, key, base_metadata = extract_event_details(event)
    print(
        json.dumps(
            {
                "message": "Received Lambda validation event.",
                "source_type": base_metadata.get("source_type"),
                "bucket": bucket,
                "key": key,
            }
        )
    )
    return validate_metadata(bucket, key, base_metadata)


def load_sample_event():
    sample_event_path = Path(__file__).with_name("sample_event.json")
    if not sample_event_path.exists():
        return {
            "bucket": "iot-data-lake",
            "key": "raw/2026/06/23/sample_iot_logs.jsonl",
        }

    with sample_event_path.open("r", encoding="utf-8") as sample_file:
        return json.load(sample_file)


if __name__ == "__main__":
    sample_response = lambda_handler(load_sample_event(), None)
    print(json.dumps(sample_response, indent=2))
