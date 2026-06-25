package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(cfg Config) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.BootstrapServers...),
			Topic:        cfg.RawTopic,
			Balancer:     &kafka.LeastBytes{},
			// Keep synchronous semantics, but avoid the kafka-go 1s default batch wait
			// so controlled medium-profile validation remains practical.
			BatchTimeout: time.Millisecond,
			RequiredAcks: kafka.RequireOne,
		},
		topic: cfg.RawTopic,
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishRecords(
	ctx context.Context,
	records []CSVRecord,
	sendDelay time.Duration,
	progressInterval int,
	progressMode string,
) (int, int) {
	var sentCount int
	var failedCount int
	totalRecords := len(records)

	for index, record := range records {
		payload, err := record.ToJSONBytes(time.Now())
		if err != nil {
			failedCount++
			prepareProgressLineForMessage(progressMode)
			fmt.Printf("failed to marshal record for device %s: %v\n", record.DeviceID, err)
			logProducerProgress(index+1, totalRecords, sentCount, failedCount, progressInterval, progressMode)
			continue
		}

		message := kafka.Message{
			Key:   []byte(record.DeviceID),
			Value: payload,
		}

		if err := p.writer.WriteMessages(ctx, message); err != nil {
			failedCount++
			prepareProgressLineForMessage(progressMode)
			fmt.Printf("failed to publish record for device %s to topic %s: %v\n", record.DeviceID, p.topic, err)
			logProducerProgress(index+1, totalRecords, sentCount, failedCount, progressInterval, progressMode)
			continue
		}

		sentCount++
		logProducerProgress(index+1, totalRecords, sentCount, failedCount, progressInterval, progressMode)

		if sendDelay > 0 {
			time.Sleep(sendDelay)
		}
	}

	return sentCount, failedCount
}

func logProducerProgress(attemptedCount int, totalRecords int, sentCount int, failedCount int, progressInterval int, progressMode string) {
	if progressInterval <= 0 {
		return
	}

	if attemptedCount%progressInterval != 0 && attemptedCount != totalRecords {
		return
	}

	if progressMode == "bar" {
		printProducerBar(attemptedCount, totalRecords, sentCount, failedCount)
		return
	}

	fmt.Printf(
		"producer progress attempted=%d total=%d sent=%d failed=%d\n",
		attemptedCount,
		totalRecords,
		sentCount,
		failedCount,
	)
}

func printProducerBar(attemptedCount int, totalRecords int, sentCount int, failedCount int) {
	width := 24
	filled := 0
	if totalRecords > 0 {
		filled = attemptedCount * width / totalRecords
	}
	if filled > width {
		filled = width
	}

	bar := strings.Repeat("=", filled) + strings.Repeat(" ", width-filled)
	fmt.Printf(
		"\rproducer [%s] %d/%d sent=%d failed=%d",
		bar,
		attemptedCount,
		totalRecords,
		sentCount,
		failedCount,
	)
	if attemptedCount == totalRecords {
		fmt.Print("\n")
	}
}

func prepareProgressLineForMessage(progressMode string) {
	if progressMode == "bar" {
		fmt.Print("\n")
	}
}
