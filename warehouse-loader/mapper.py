import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class MappingError(ValueError):
    pass


@dataclass(frozen=True)
class ProcessedLogRow:
    event_timestamp: datetime
    device_id: str
    source_ip: str
    destination_ip: str
    protocol: str
    packet_size: int
    duration_ms: int
    event_type: str
    attack_type: str | None
    status: str
    ingestion_timestamp: datetime | None
    processed_at: datetime
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class InvalidLogRow:
    raw_payload: str
    error_reason: str
    failed_at: datetime


def map_processed_message(raw_payload: str | bytes) -> ProcessedLogRow:
    payload = _load_json_object(raw_payload)

    return ProcessedLogRow(
        event_timestamp=_parse_required_timestamp(payload, "event_timestamp"),
        device_id=_get_required_text(payload, "device_id"),
        source_ip=_get_required_text(payload, "source_ip"),
        destination_ip=_get_required_text(payload, "destination_ip"),
        protocol=_get_required_text(payload, "protocol"),
        packet_size=_get_required_int(payload, "packet_size"),
        duration_ms=_get_required_int(payload, "duration_ms"),
        event_type=_get_required_text(payload, "event_type"),
        attack_type=_get_optional_text(payload, "attack_type"),
        status=_get_required_text(payload, "status"),
        ingestion_timestamp=_parse_optional_timestamp(payload, "ingestion_timestamp"),
        processed_at=_parse_required_timestamp(payload, "processed_at"),
        raw_payload=payload,
    )


def map_invalid_message(raw_payload: str | bytes) -> InvalidLogRow:
    payload = _load_json_object(raw_payload)

    return InvalidLogRow(
        raw_payload=_get_required_text(payload, "raw_payload"),
        error_reason=_get_required_text(payload, "error_reason"),
        failed_at=_parse_required_timestamp(payload, "failed_at"),
    )


def _load_json_object(raw_payload: str | bytes) -> dict[str, Any]:
    raw_text = _to_text(raw_payload)

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise MappingError(f"invalid JSON: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise MappingError("payload must be a JSON object")

    return payload


def _get_required_text(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if value is None:
        raise MappingError(f"missing required field: {field_name}")

    text = str(value).strip()
    if not text:
        raise MappingError(f"missing required field: {field_name}")

    return text


def _get_optional_text(payload: dict[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _get_required_int(payload: dict[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if value is None or isinstance(value, bool):
        raise MappingError(f"{field_name} must be an integer")

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise MappingError(f"{field_name} must be an integer") from exc


def _parse_required_timestamp(payload: dict[str, Any], field_name: str) -> datetime:
    value = payload.get(field_name)
    if value is None:
        raise MappingError(f"missing required field: {field_name}")

    return _parse_timestamp(value, field_name)


def _parse_optional_timestamp(payload: dict[str, Any], field_name: str) -> datetime | None:
    value = payload.get(field_name)
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return _parse_timestamp(text, field_name)


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise MappingError(f"{field_name} must be a string")

    candidate = value.strip()
    if not candidate:
        raise MappingError(f"{field_name} must not be empty")

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MappingError(f"invalid timestamp for {field_name}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _to_text(raw_payload: str | bytes) -> str:
    if isinstance(raw_payload, bytes):
        return raw_payload.decode("utf-8", errors="replace")
    return str(raw_payload)
