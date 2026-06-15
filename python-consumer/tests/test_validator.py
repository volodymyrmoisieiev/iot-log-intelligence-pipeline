import json

from validator import build_invalid_record, validate_message


def test_validate_message_accepts_valid_record():
    payload = json.dumps(
        {
            "event_timestamp": "2025-01-01T00:00:00Z",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "udp",
            "packet_size": 118,
            "duration_ms": 1,
            "event_type": "attack",
            "status": "failed",
            "attack_type": "ARP_poisioning",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is True
    assert result.error_reason is None
    assert result.normalized_record is not None
    assert result.normalized_record["protocol"] == "UDP"
    assert result.normalized_record["event_type"] == "attack"
    assert result.normalized_record["packet_size"] == 118
    assert result.normalized_record["duration_ms"] == 1
    assert result.normalized_record["processed_at"].endswith("Z")


def test_validate_message_rejects_missing_required_field():
    payload = json.dumps(
        {
            "event_timestamp": "2025-01-01T00:00:00Z",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "udp",
            "packet_size": 118,
            "duration_ms": 1,
            "event_type": "attack",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is False
    assert result.error_reason == "missing required field: status"


def test_validate_message_rejects_invalid_timestamp():
    payload = json.dumps(
        {
            "event_timestamp": "not-a-timestamp",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "udp",
            "packet_size": 118,
            "duration_ms": 1,
            "event_type": "attack",
            "status": "failed",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is False
    assert result.error_reason == "invalid timestamp"


def test_validate_message_rejects_invalid_protocol():
    payload = json.dumps(
        {
            "event_timestamp": "2025-01-01T00:00:00Z",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "http",
            "packet_size": 118,
            "duration_ms": 1,
            "event_type": "attack",
            "status": "failed",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is False
    assert result.error_reason == "invalid protocol"


def test_validate_message_rejects_negative_packet_size():
    payload = json.dumps(
        {
            "event_timestamp": "2025-01-01T00:00:00Z",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "udp",
            "packet_size": -1,
            "duration_ms": 1,
            "event_type": "attack",
            "status": "failed",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is False
    assert result.error_reason == "packet_size must be a non-negative integer"


def test_validate_message_rejects_negative_duration_ms():
    payload = json.dumps(
        {
            "event_timestamp": "2025-01-01T00:00:00Z",
            "device_id": "device_001",
            "source_ip": "10.10.103.1",
            "destination_ip": "192.168.1.104",
            "protocol": "udp",
            "packet_size": 118,
            "duration_ms": -5,
            "event_type": "attack",
            "status": "failed",
        }
    )

    result = validate_message(payload)

    assert result.is_valid is False
    assert result.error_reason == "duration_ms must be a non-negative integer"


def test_validate_message_rejects_invalid_json():
    result = validate_message('{"event_timestamp": ')

    assert result.is_valid is False
    assert result.error_reason is not None
    assert result.error_reason.startswith("invalid JSON:")


def test_build_invalid_record_preserves_payload_and_reason():
    record = build_invalid_record("not-json", "invalid JSON")

    assert record["raw_payload"] == "not-json"
    assert record["error_reason"] == "invalid JSON"
    assert record["failed_at"].endswith("Z")
