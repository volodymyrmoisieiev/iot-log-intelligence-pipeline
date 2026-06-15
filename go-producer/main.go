package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "go producer failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	fmt.Printf("starting Go producer with input=%s topic=%s brokers=%v delay_ms=%d\n", cfg.InputFile, cfg.RawTopic, cfg.BootstrapServers, cfg.SendDelayMS)

	records, skippedCount, err := ReadCSVRecords(cfg.InputFile)
	if err != nil {
		return err
	}

	fmt.Printf("loaded %d records from csv, skipped %d invalid records\n", len(records), skippedCount)

	producer := NewProducer(cfg)
	defer producer.Close()

	sentCount, failedCount := producer.PublishRecords(context.Background(), records, time.Duration(cfg.SendDelayMS)*time.Millisecond)

	fmt.Printf("finished publishing to topic %s: sent=%d failed=%d skipped=%d\n", cfg.RawTopic, sentCount, failedCount, skippedCount)

	if sentCount == 0 && len(records) > 0 {
		return fmt.Errorf("no records were published successfully")
	}

	return nil
}
