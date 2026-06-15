package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultContainerBootstrapServers = "kafka:9092"
	defaultLocalBootstrapServers     = "localhost:29092"
	defaultRawTopic                  = "iot_raw_logs"
	defaultSendDelayMS               = 250
)

type Config struct {
	BootstrapServers []string
	RawTopic         string
	InputFile        string
	SendDelayMS      int
}

func LoadConfig() (Config, error) {
	delayMS, err := getEnvInt("PRODUCER_SEND_DELAY_MS", defaultSendDelayMS)
	if err != nil {
		return Config{}, err
	}

	return Config{
		BootstrapServers: getBootstrapServers(),
		RawTopic:         getEnv("KAFKA_RAW_TOPIC", defaultRawTopic),
		InputFile:        getInputFile(),
		SendDelayMS:      delayMS,
	}, nil
}

func getBootstrapServers() []string {
	raw := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	if raw == "" {
		raw = getDefaultBootstrapServers()
	}
	parts := strings.Split(raw, ",")
	servers := make([]string, 0, len(parts))

	for _, part := range parts {
		server := strings.TrimSpace(part)
		if server == "" {
			continue
		}
		servers = append(servers, server)
	}

	if len(servers) == 0 {
		return []string{getDefaultBootstrapServers()}
	}

	return servers
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func getInputFile() string {
	value := strings.TrimSpace(os.Getenv("PRODUCER_INPUT_FILE"))
	if value != "" {
		return value
	}

	candidates := []string{
		"/app/data/samples/sample_iot_logs.csv",
		"../data/samples/sample_iot_logs.csv",
		"data/samples/sample_iot_logs.csv",
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	return candidates[0]
}

func getDefaultBootstrapServers() string {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return defaultContainerBootstrapServers
	}

	return defaultLocalBootstrapServers
}

func getEnvInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}

	if parsed < 0 {
		return 0, fmt.Errorf("%s must be zero or greater", key)
	}

	return parsed, nil
}
