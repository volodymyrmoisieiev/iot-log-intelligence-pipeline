import json
from datetime import timezone

from mapper import map_invalid_message, map_processed_message


def test_map_processed_message_accepts_valid_record():
    row = map_processed_message(
        """
        {
          "event_timestamp": "2025-01-01T00:00:00Z",
          "device_id": "device_001",
          "source_ip": "10.10.103.1",
          "destination_ip": "192.168.1.104",
          "protocol": "UDP",
          "packet_size": 118,
          "duration_ms": 1,
          "event_type": "attack",
          "attack_type": "ARP_poisoning",
          "status": "failed",
          "ingestion_timestamp": "2025-01-01T00:00:01Z",
          "processed_at": "2025-01-01T00:00:02Z"
        }
        """
    )

    assert row.device_id == "device_001"
    assert row.attack_type == "ARP_poisoning"
    assert row.ingestion_timestamp is not None
    assert row.ingestion_timestamp.tzinfo == timezone.utc
    assert row.raw_payload["device_id"] == "device_001"


def test_map_invalid_message_accepts_invalid_record():
    row = map_invalid_message(
        json.dumps(
            {
                "raw_payload": '{"event_timestamp": }',
                "error_reason": "invalid JSON: Expecting value",
                "failed_at": "2025-01-01T00:00:03Z",
            }
        )
    )

    assert row.raw_payload == "{\"event_timestamp\": }"
    assert row.error_reason == "invalid JSON: Expecting value"
    assert row.failed_at.tzinfo == timezone.utc


def test_map_processed_message_allows_missing_optional_attack_type():
    row = map_processed_message(
        """
        {
          "event_timestamp": "2025-01-01T00:00:00Z",
          "device_id": "device_002",
          "source_ip": "10.10.103.2",
          "destination_ip": "192.168.1.105",
          "protocol": "TCP",
          "packet_size": 256,
          "duration_ms": 4,
          "event_type": "normal",
          "status": "success",
          "ingestion_timestamp": "2025-01-01T00:00:01Z",
          "processed_at": "2025-01-01T00:00:02Z"
        }
        """
    )

    assert row.attack_type is None


def test_map_processed_message_allows_missing_optional_ingestion_timestamp():
    row = map_processed_message(
        """
        {
          "event_timestamp": "2025-01-01T00:00:00Z",
          "device_id": "device_003",
          "source_ip": "10.10.103.3",
          "destination_ip": "192.168.1.106",
          "protocol": "ICMP",
          "packet_size": 64,
          "duration_ms": 0,
          "event_type": "warning",
          "attack_type": "",
          "status": "success",
          "processed_at": "2025-01-01T00:00:02Z"
        }
        """
    )

    assert row.ingestion_timestamp is None
    assert row.attack_type is None
