package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultContainerBootstrapServers = "kafka:9092"
	defaultLocalBootstrapServers     = "localhost:29092"
	defaultRawTopic                  = "iot_raw_logs"
	defaultDatasetProfile            = "sample"
	defaultSendDelayMS               = 250
	defaultProducerMaxRows           = 0
	defaultProducerProgressInterval  = 1000
	datasetProfileSample             = "sample"
	datasetProfileMedium             = "medium"
	datasetProfileFull               = "full"
)

type Config struct {
	BootstrapServers []string
	RawTopic         string
	DatasetProfile   string
	InputFile        string
	MaxRows          int
	SendDelayMS      int
	ProgressInterval int
}

var datasetProfilePaths = map[string]string{
	datasetProfileSample: "/app/data/samples/sample_iot_logs.csv",
	datasetProfileMedium: "/app/data/processed/medium_iot_logs.csv",
	datasetProfileFull:   "/app/data/raw/full_iot_logs.csv",
}

var datasetProfileRelativePaths = map[string]string{
	datasetProfileSample: "data/samples/sample_iot_logs.csv",
	datasetProfileMedium: "data/processed/medium_iot_logs.csv",
	datasetProfileFull:   "data/raw/full_iot_logs.csv",
}

func LoadConfig() (Config, error) {
	datasetProfile, err := getDatasetProfile()
	if err != nil {
		return Config{}, err
	}

	delayMS, err := getEnvInt("PRODUCER_SEND_DELAY_MS", defaultSendDelayMS)
	if err != nil {
		return Config{}, err
	}

	maxRows, err := getEnvInt("PRODUCER_MAX_ROWS", defaultProducerMaxRows)
	if err != nil {
		return Config{}, err
	}

	progressInterval, err := getEnvPositiveInt("PRODUCER_PROGRESS_INTERVAL", defaultProducerProgressInterval)
	if err != nil {
		return Config{}, err
	}

	inputFile, err := resolveInputFile(
		datasetProfile,
		strings.TrimSpace(os.Getenv("PRODUCER_INPUT_FILE")),
		fileExists,
	)
	if err != nil {
		return Config{}, err
	}

	return Config{
		BootstrapServers: getBootstrapServers(),
		RawTopic:         getEnv("KAFKA_RAW_TOPIC", defaultRawTopic),
		DatasetProfile:   datasetProfile,
		InputFile:        inputFile,
		MaxRows:          maxRows,
		SendDelayMS:      delayMS,
		ProgressInterval: progressInterval,
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

func getDatasetProfile() (string, error) {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("DATASET_PROFILE")))
	if value == "" {
		return defaultDatasetProfile, nil
	}

	if _, ok := datasetProfilePaths[value]; !ok {
		return "", fmt.Errorf(
			"DATASET_PROFILE=%q is invalid; allowed values are sample, medium, full",
			value,
		)
	}

	return value, nil
}

func resolveInputFile(profile string, explicitInputFile string, exists func(string) bool) (string, error) {
	if explicitInputFile != "" {
		if !exists(explicitInputFile) {
			return "", fmt.Errorf(
				"PRODUCER_INPUT_FILE points to %s, but the file was not found",
				explicitInputFile,
			)
		}

		return explicitInputFile, nil
	}

	return resolveProfileInputFile(profile, exists)
}

func resolveProfileInputFile(profile string, exists func(string) bool) (string, error) {
	containerPath, ok := datasetProfilePaths[profile]
	if !ok {
		return "", fmt.Errorf("unsupported dataset profile: %s", profile)
	}

	for _, candidate := range datasetProfileCandidates(profile) {
		if exists(candidate) {
			return candidate, nil
		}
	}

	return "", datasetProfileMissingError(profile, containerPath)
}

func datasetProfileCandidates(profile string) []string {
	relativePath := filepath.FromSlash(datasetProfileRelativePaths[profile])

	return []string{
		datasetProfilePaths[profile],
		relativePath,
		filepath.Join("..", relativePath),
	}
}

func datasetProfileMissingError(profile string, resolvedPath string) error {
	switch profile {
	case datasetProfileMedium:
		return fmt.Errorf(
			"DATASET_PROFILE=medium resolved to %s, but the file was not found. Generate it first with: python .\\scripts\\create_dataset_profile.py --input .\\data\\raw\\RT_IOT2022.csv --output .\\data\\processed\\medium_iot_logs.csv --rows 10000",
			resolvedPath,
		)
	case datasetProfileFull:
		return fmt.Errorf(
			"DATASET_PROFILE=full resolved to %s, but the file was not found. Place the full dataset at data/raw/full_iot_logs.csv before running the producer.",
			resolvedPath,
		)
	default:
		return fmt.Errorf(
			"DATASET_PROFILE=%s resolved to %s, but the file was not found",
			profile,
			resolvedPath,
		)
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
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

func getEnvPositiveInt(key string, fallback int) (int, error) {
	value, err := getEnvInt(key, fallback)
	if err != nil {
		return 0, err
	}

	if value <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}

	return value, nil
}
