package main

import (
	"context"
	"fmt"
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
			RequiredAcks: kafka.RequireOne,
		},
		topic: cfg.RawTopic,
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishRecords(ctx context.Context, records []CSVRecord, sendDelay time.Duration) (int, int) {
	var sentCount int
	var failedCount int

	for _, record := range records {
		payload, err := record.ToJSONBytes(time.Now())
		if err != nil {
			failedCount++
			fmt.Printf("failed to marshal record for device %s: %v\n", record.DeviceID, err)
			continue
		}

		message := kafka.Message{
			Key:   []byte(record.DeviceID),
			Value: payload,
		}

		if err := p.writer.WriteMessages(ctx, message); err != nil {
			failedCount++
			fmt.Printf("failed to publish record for device %s to topic %s: %v\n", record.DeviceID, p.topic, err)
			continue
		}

		sentCount++

		if sendDelay > 0 {
			time.Sleep(sendDelay)
		}
	}

	return sentCount, failedCount
}
