package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadCSVRecords(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sample.csv")

	content := "event_timestamp,device_id,source_ip,destination_ip,protocol,packet_size,duration_ms,event_type,attack_type,status\n" +
		"2025-01-01T00:00:00Z,device_001,10.0.0.1,192.168.1.10,tcp,120,42,normal,None,success\n" +
		"2025-01-01T00:01:00Z,device_002,10.0.0.2,192.168.1.11,udp,invalid,10,attack,DOS_SYN,blocked\n" +
		"2025-01-01T00:02:00Z,device_003,10.0.0.3,192.168.1.12,udp,256,99,attack,ARP_poisioning,failed\n"

	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}

	records, skipped, err := ReadCSVRecords(filePath)
	if err != nil {
		t.Fatalf("ReadCSVRecords returned error: %v", err)
	}

	if skipped != 1 {
		t.Fatalf("expected 1 skipped record, got %d", skipped)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 parsed records, got %d", len(records))
	}

	if records[0].DeviceID != "device_001" {
		t.Fatalf("unexpected first device id: %s", records[0].DeviceID)
	}

	if records[1].PacketSize != 256 {
		t.Fatalf("unexpected packet size: %d", records[1].PacketSize)
	}
}

func TestCSVRecordToJSONMessage(t *testing.T) {
	t.Parallel()

	record := CSVRecord{
		EventTimestamp: "2025-01-01T00:00:00Z",
		DeviceID:       "device_007",
		SourceIP:       "10.0.0.7",
		DestinationIP:  "192.168.1.7",
		Protocol:       "tcp",
		PacketSize:     512,
		DurationMS:     250,
		EventType:      "attack",
		AttackType:     "DOS_SYN",
		Status:         "blocked",
	}

	now := time.Date(2025, time.January, 1, 12, 0, 0, 0, time.UTC)
	payload, err := record.ToJSONBytes(now)
	if err != nil {
		t.Fatalf("ToJSONBytes returned error: %v", err)
	}

	var message JSONMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}

	if message.DeviceID != record.DeviceID {
		t.Fatalf("expected device id %s, got %s", record.DeviceID, message.DeviceID)
	}

	if message.IngestionTimestamp != "2025-01-01T12:00:00Z" {
		t.Fatalf("unexpected ingestion timestamp: %s", message.IngestionTimestamp)
	}

	if message.AttackType != "DOS_SYN" {
		t.Fatalf("unexpected attack type: %s", message.AttackType)
	}
}
