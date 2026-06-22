# Dataset Profiles

## Overview

Stage 15A adds a dataset profile foundation so later stages can switch between documented dataset sizes without changing the repository's default lightweight behavior.

The current pipeline still uses the tracked sample CSV at `data/samples/sample_iot_logs.csv`. This stage does not change the Go producer, Python consumer, warehouse loader, Airflow DAGs, or dbt models yet. It only defines the profile contract those later stages can reuse.

Profile metadata lives in `data/dataset_profiles.yml`.

## Profiles

### `sample`

- path: `data/samples/sample_iot_logs.csv`
- purpose: quick local demo runs, smoke checks, and CI-safe validation
- git policy: committed to the repository
- CI safety: safe for CI because it is intentionally tiny and predictable

Use `sample` when you want the fastest feedback loop and when repository validation must stay small, deterministic, and inexpensive.

### `medium`

- path: `data/processed/medium_iot_logs.csv`
- purpose: integration testing on a larger but still curated subset
- git policy: not committed by default
- CI safety: not considered CI-safe unless a future workflow explicitly documents and provisions it

Use `medium` when the sample dataset is too small to exercise realistic record volume, but a full raw-dataset run would be too heavy for normal local iteration.

### `full`

- path: `data/raw/full_iot_logs.csv`
- purpose: full raw-dataset processing for local/manual or cloud-style validation
- git policy: never commit the raw full dataset to normal repository history
- CI safety: not safe for CI

Use `full` only for intentional larger-scale validation. It is expected to be slower, heavier, and more environment-specific than the tracked sample workflow.

## Why the Modes Exist

`sample` exists so demos, smoke tests, and CI checks stay fast and stable.

`medium` exists so future integration validation can exercise more realistic data volume without requiring the whole raw dataset.

`full` exists so future end-to-end validation can be tested on the complete source data when a developer or cloud-style environment is ready for that cost.

## Git and Data Governance

Large raw and generated datasets should not be committed because they make clones heavier, increase review noise, and create avoidable repository churn.

The repository keeps only the tiny sample CSV under version control. Planned future larger inputs such as `data/processed/medium_iot_logs.csv` and `data/raw/full_iot_logs.csv` are intentionally ignored by git.

## How Future Stages Will Use This

Later Stage 15 work can wire these documented profiles into:

- Go producer input selection
- Python consumer and warehouse-loader validation runs
- Airflow orchestration options
- larger local or cloud-style dataset processing flows

Stage 15A is only the shared configuration and documentation layer for that future wiring.

## Validation Commands

Verify the profile file and tracked sample dataset exist:

```powershell
Test-Path .\data\samples\sample_iot_logs.csv
Test-Path .\data\dataset_profiles.yml
```

Verify the planned larger dataset paths are ignored by git:

```powershell
git check-ignore -v .\data\raw\full_iot_logs.csv
git check-ignore -v .\data\processed\medium_iot_logs.csv
```

If `git check-ignore` does not report a result for a missing path in your environment, create temporary empty files, verify the ignore rules, then delete them:

```powershell
New-Item -ItemType File -Path .\data\raw\full_iot_logs.csv -Force
New-Item -ItemType File -Path .\data\processed\medium_iot_logs.csv -Force
git check-ignore -v .\data\raw\full_iot_logs.csv
git check-ignore -v .\data\processed\medium_iot_logs.csv
Remove-Item .\data\raw\full_iot_logs.csv
Remove-Item .\data\processed\medium_iot_logs.csv
```

Confirm validation does not leave repository noise behind:

```powershell
git status
```
