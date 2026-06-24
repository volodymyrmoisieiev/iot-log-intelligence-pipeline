package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveInputFileUsesProfilePath(t *testing.T) {
	t.Parallel()

	expectedPath := filepath.FromSlash("data/processed/medium_iot_logs.csv")
	inputFile, err := resolveInputFile(datasetProfileMedium, "", func(path string) bool {
		return path == expectedPath
	})
	if err != nil {
		t.Fatalf("resolveInputFile returned error: %v", err)
	}

	if inputFile != expectedPath {
		t.Fatalf("expected profile path %s, got %s", expectedPath, inputFile)
	}
}

func TestResolveInputFileHonorsExplicitOverride(t *testing.T) {
	t.Parallel()

	overridePath := filepath.FromSlash("custom/input.csv")
	inputFile, err := resolveInputFile(datasetProfileSample, overridePath, func(path string) bool {
		return path == overridePath
	})
	if err != nil {
		t.Fatalf("resolveInputFile returned error: %v", err)
	}

	if inputFile != overridePath {
		t.Fatalf("expected override path %s, got %s", overridePath, inputFile)
	}
}

func TestGetDatasetProfileRejectsInvalidValue(t *testing.T) {
	t.Setenv("DATASET_PROFILE", "archive")

	_, err := getDatasetProfile()
	if err == nil {
		t.Fatal("expected getDatasetProfile to fail for invalid profile")
	}

	if !strings.Contains(err.Error(), "allowed values are sample, medium, full") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigParsesProducerMaxRows(t *testing.T) {
	tempDir := t.TempDir()
	inputFile := filepath.Join(tempDir, "sample.csv")
	if err := writeTempFile(inputFile); err != nil {
		t.Fatalf("writeTempFile returned error: %v", err)
	}

	t.Setenv("PRODUCER_INPUT_FILE", inputFile)
	t.Setenv("PRODUCER_MAX_ROWS", "15")
	t.Setenv("DATASET_PROFILE", "full")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.InputFile != inputFile {
		t.Fatalf("expected input file %s, got %s", inputFile, cfg.InputFile)
	}

	if cfg.MaxRows != 15 {
		t.Fatalf("expected max rows 15, got %d", cfg.MaxRows)
	}

	if cfg.ProgressInterval != defaultProducerProgressInterval {
		t.Fatalf("expected default progress interval %d, got %d", defaultProducerProgressInterval, cfg.ProgressInterval)
	}

	if cfg.DatasetProfile != datasetProfileFull {
		t.Fatalf("expected dataset profile %s, got %s", datasetProfileFull, cfg.DatasetProfile)
	}
}

func TestLoadConfigParsesProducerProgressInterval(t *testing.T) {
	tempDir := t.TempDir()
	inputFile := filepath.Join(tempDir, "sample.csv")
	if err := writeTempFile(inputFile); err != nil {
		t.Fatalf("writeTempFile returned error: %v", err)
	}

	t.Setenv("PRODUCER_INPUT_FILE", inputFile)
	t.Setenv("PRODUCER_PROGRESS_INTERVAL", "250")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.ProgressInterval != 250 {
		t.Fatalf("expected progress interval 250, got %d", cfg.ProgressInterval)
	}
}

func TestLoadConfigRejectsInvalidProducerProgressInterval(t *testing.T) {
	tempDir := t.TempDir()
	inputFile := filepath.Join(tempDir, "sample.csv")
	if err := writeTempFile(inputFile); err != nil {
		t.Fatalf("writeTempFile returned error: %v", err)
	}

	t.Setenv("PRODUCER_INPUT_FILE", inputFile)
	t.Setenv("PRODUCER_PROGRESS_INTERVAL", "0")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected LoadConfig to reject zero progress interval")
	}

	if !strings.Contains(err.Error(), "PRODUCER_PROGRESS_INTERVAL must be greater than zero") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveInputFileMissingMediumIncludesGenerationGuidance(t *testing.T) {
	t.Parallel()

	_, err := resolveInputFile(datasetProfileMedium, "", func(string) bool {
		return false
	})
	if err == nil {
		t.Fatal("expected resolveInputFile to fail when medium file is missing")
	}

	if !strings.Contains(err.Error(), "create_dataset_profile.py") {
		t.Fatalf("expected generation guidance in error, got: %v", err)
	}
}

func writeTempFile(path string) error {
	content := []byte("placeholder")
	return os.WriteFile(path, content, 0o600)
}
