import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


REQUIRED_FIELDS = (
    "event_timestamp",
    "device_id",
    "source_ip",
    "destination_ip",
    "protocol",
    "packet_size",
    "duration_ms",
    "event_type",
    "status",
)

ALLOWED_PROTOCOLS = {"TCP", "UDP", "ICMP"}
ALLOWED_EVENT_TYPES = {"normal", "warning", "error", "attack"}


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    normalized_record: dict[str, Any] | None = None
    error_reason: str | None = None
    raw_payload: str | None = None


def validate_message(raw_payload: str | bytes) -> ValidationResult:
    raw_text = _to_text(raw_payload)

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return ValidationResult(
            is_valid=False,
            error_reason=f"invalid JSON: {exc.msg}",
            raw_payload=raw_text,
        )

    if not isinstance(payload, dict):
        return ValidationResult(
            is_valid=False,
            error_reason="payload must be a JSON object",
            raw_payload=raw_text,
        )

    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in payload or _is_blank(payload[field]):
            errors.append(f"missing required field: {field}")

    if errors:
        return ValidationResult(
            is_valid=False,
            error_reason="; ".join(errors),
            raw_payload=raw_text,
        )

    timestamp_value = _normalize_timestamp(payload["event_timestamp"], errors)
    protocol_value = _normalize_protocol(payload["protocol"], errors)
    packet_size_value = _normalize_non_negative_int(payload["packet_size"], "packet_size", errors)
    duration_ms_value = _normalize_non_negative_int(payload["duration_ms"], "duration_ms", errors)
    event_type_value = _normalize_event_type(payload["event_type"], errors)

    if errors:
        return ValidationResult(
            is_valid=False,
            error_reason="; ".join(errors),
            raw_payload=raw_text,
        )

    normalized = dict(payload)
    normalized["event_timestamp"] = timestamp_value
    normalized["device_id"] = str(payload["device_id"]).strip()
    normalized["source_ip"] = str(payload["source_ip"]).strip()
    normalized["destination_ip"] = str(payload["destination_ip"]).strip()
    normalized["protocol"] = protocol_value
    normalized["packet_size"] = packet_size_value
    normalized["duration_ms"] = duration_ms_value
    normalized["event_type"] = event_type_value
    normalized["status"] = str(payload["status"]).strip().lower()
    normalized["processed_at"] = _utc_now_iso()

    return ValidationResult(
        is_valid=True,
        normalized_record=normalized,
        raw_payload=raw_text,
    )


def build_invalid_record(raw_payload: str | bytes, error_reason: str) -> dict[str, str]:
    return {
        "raw_payload": _to_text(raw_payload),
        "error_reason": error_reason,
        "failed_at": _utc_now_iso(),
    }


def _normalize_timestamp(value: Any, errors: list[str]) -> str | None:
    if not isinstance(value, str):
        errors.append("event_timestamp must be a string")
        return None

    candidate = value.strip()
    if not candidate:
        errors.append("event_timestamp must not be empty")
        return None

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        errors.append("invalid timestamp")
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_protocol(value: Any, errors: list[str]) -> str | None:
    protocol = str(value).strip().upper()
    if protocol not in ALLOWED_PROTOCOLS:
        errors.append("invalid protocol")
        return None
    return protocol


def _normalize_non_negative_int(value: Any, field_name: str, errors: list[str]) -> int | None:
    if isinstance(value, bool):
        errors.append(f"{field_name} must be a non-negative integer")
        return None

    try:
        normalized = int(value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} must be a non-negative integer")
        return None

    if normalized < 0:
        errors.append(f"{field_name} must be a non-negative integer")
        return None

    return normalized


def _normalize_event_type(value: Any, errors: list[str]) -> str | None:
    event_type = str(value).strip().lower()
    if event_type not in ALLOWED_EVENT_TYPES:
        errors.append("invalid event_type")
        return None
    return event_type


def _to_text(raw_payload: str | bytes) -> str:
    if isinstance(raw_payload, bytes):
        return raw_payload.decode("utf-8", errors="replace")
    return str(raw_payload)


def _is_blank(value: Any) -> bool:
    return isinstance(value, str) and not value.strip()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
