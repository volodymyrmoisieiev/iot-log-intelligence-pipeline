package main

import (
	"context"
	"fmt"
	"os"
	"strings"
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

	fmt.Println("starting Go producer")
	fmt.Printf("dataset profile: %s\n", cfg.DatasetProfile)
	fmt.Printf("resolved input file: %s\n", cfg.InputFile)
	fmt.Printf("max rows: %d\n", cfg.MaxRows)
	fmt.Printf("send delay ms: %d\n", cfg.SendDelayMS)
	fmt.Printf("progress interval: %d\n", cfg.ProgressInterval)
	fmt.Printf("progress mode: %s\n", cfg.ProgressMode)
	fmt.Printf("kafka bootstrap servers: %s\n", strings.Join(cfg.BootstrapServers, ","))
	fmt.Printf("target topic: %s\n", cfg.RawTopic)

	records, skippedCount, err := ReadCSVRecords(cfg.InputFile, cfg.MaxRows)
	if err != nil {
		return err
	}

	fmt.Printf("loaded %d records from csv, skipped %d invalid records\n", len(records), skippedCount)

	producer := NewProducer(cfg)
	defer producer.Close()

	sentCount, failedCount := producer.PublishRecords(
		context.Background(),
		records,
		time.Duration(cfg.SendDelayMS)*time.Millisecond,
		cfg.ProgressInterval,
		cfg.ProgressMode,
	)

	fmt.Printf("finished publishing to topic %s: sent=%d failed=%d skipped=%d\n", cfg.RawTopic, sentCount, failedCount, skippedCount)

	if sentCount == 0 && len(records) > 0 {
		return fmt.Errorf("no records were published successfully")
	}

	return nil
}
