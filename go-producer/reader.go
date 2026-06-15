package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type CSVRecord struct {
	EventTimestamp string
	DeviceID       string
	SourceIP       string
	DestinationIP  string
	Protocol       string
	PacketSize     int
	DurationMS     int
	EventType      string
	AttackType     string
	Status         string
}

type JSONMessage struct {
	EventTimestamp     string `json:"event_timestamp"`
	DeviceID           string `json:"device_id"`
	SourceIP           string `json:"source_ip"`
	DestinationIP      string `json:"destination_ip"`
	Protocol           string `json:"protocol"`
	PacketSize         int    `json:"packet_size"`
	DurationMS         int    `json:"duration_ms"`
	EventType          string `json:"event_type"`
	AttackType         string `json:"attack_type"`
	Status             string `json:"status"`
	IngestionTimestamp string `json:"ingestion_timestamp"`
}

func ReadCSVRecords(path string) ([]CSVRecord, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open csv file: %w", err)
	}
	defer file.Close()

	bufferedReader := bufio.NewReader(file)
	if bom, err := bufferedReader.Peek(3); err == nil && len(bom) == 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		if _, err := bufferedReader.Discard(3); err != nil {
			return nil, 0, fmt.Errorf("discard csv bom: %w", err)
		}
	}

	reader := csv.NewReader(bufferedReader)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("read csv header: %w", err)
	}

	indexByName := make(map[string]int, len(header))
	for idx, name := range header {
		indexByName[strings.TrimSpace(name)] = idx
	}

	required := []string{
		"event_timestamp",
		"device_id",
		"source_ip",
		"destination_ip",
		"protocol",
		"packet_size",
		"duration_ms",
		"event_type",
		"attack_type",
		"status",
	}

	for _, field := range required {
		if _, ok := indexByName[field]; !ok {
			return nil, 0, fmt.Errorf("missing required csv column %q", field)
		}
	}

	var (
		records []CSVRecord
		skipped int
	)

	rowNumber := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			skipped++
			rowNumber++
			fmt.Printf("skipping csv row %d: %v\n", rowNumber, err)
			continue
		}

		record, err := parseCSVRecord(row, indexByName)
		if err != nil {
			skipped++
			rowNumber++
			fmt.Printf("skipping csv row %d: %v\n", rowNumber, err)
			continue
		}

		records = append(records, record)
		rowNumber++
	}

	return records, skipped, nil
}

func parseCSVRecord(row []string, indexByName map[string]int) (CSVRecord, error) {
	if len(row) < len(indexByName) {
		return CSVRecord{}, fmt.Errorf("row has %d columns, expected at least %d", len(row), len(indexByName))
	}

	packetSize, err := strconv.Atoi(strings.TrimSpace(row[indexByName["packet_size"]]))
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid packet_size: %w", err)
	}

	durationMS, err := strconv.Atoi(strings.TrimSpace(row[indexByName["duration_ms"]]))
	if err != nil {
		return CSVRecord{}, fmt.Errorf("invalid duration_ms: %w", err)
	}

	return CSVRecord{
		EventTimestamp: strings.TrimSpace(row[indexByName["event_timestamp"]]),
		DeviceID:       strings.TrimSpace(row[indexByName["device_id"]]),
		SourceIP:       strings.TrimSpace(row[indexByName["source_ip"]]),
		DestinationIP:  strings.TrimSpace(row[indexByName["destination_ip"]]),
		Protocol:       strings.TrimSpace(row[indexByName["protocol"]]),
		PacketSize:     packetSize,
		DurationMS:     durationMS,
		EventType:      strings.TrimSpace(row[indexByName["event_type"]]),
		AttackType:     strings.TrimSpace(row[indexByName["attack_type"]]),
		Status:         strings.TrimSpace(row[indexByName["status"]]),
	}, nil
}

func (r CSVRecord) ToJSONMessage(now time.Time) JSONMessage {
	return JSONMessage{
		EventTimestamp:     r.EventTimestamp,
		DeviceID:           r.DeviceID,
		SourceIP:           r.SourceIP,
		DestinationIP:      r.DestinationIP,
		Protocol:           r.Protocol,
		PacketSize:         r.PacketSize,
		DurationMS:         r.DurationMS,
		EventType:          r.EventType,
		AttackType:         r.AttackType,
		Status:             r.Status,
		IngestionTimestamp: now.UTC().Format(time.RFC3339),
	}
}

func (r CSVRecord) ToJSONBytes(now time.Time) ([]byte, error) {
	return json.Marshal(r.ToJSONMessage(now))
}
