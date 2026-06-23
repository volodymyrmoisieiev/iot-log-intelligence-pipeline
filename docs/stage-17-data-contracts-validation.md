# Stage 17: Data Contracts Validation

## Overview

Stage 17 adds the first formal data contract foundation for the raw IoT log dataset, a local CSV validator script, and an Airflow pre-check that enforces the contract before producer execution.

The Stage 17 deliverables are:

- raw CSV contract definition in `contracts/iot_raw_log_contract.yml`
- local validator script in `scripts/validate_data_contract.py`
- Airflow task integration through `validate_raw_data_contract`
- supporting documentation for local use and PR review

## Why Data Contracts Matter In This Pipeline

This project moves IoT data across multiple layers:

- raw CSV input
- Go producer
- Kafka
- Python consumer validation
- warehouse loading
- dbt modeling
- Airflow orchestration
- Spark feature generation
- observability reporting

Without a shared contract for the raw input, downstream layers can drift away from the actual source shape. A broken column name, missing required field, or unexpected raw value type can then surface later and be harder to diagnose.

Stage 17 reduces that risk by creating an explicit schema contract and validating it early.

## Key Stage 17 Locations

- raw contract: `contracts/iot_raw_log_contract.yml`
- validator script: `scripts/validate_data_contract.py`
- detailed contract guide: `docs/data-contracts.md`
- Airflow orchestration DAG: `airflow/dags/iot_local_pipeline_dag.py`

## Airflow Integration

Stage 17C wires the validator into Airflow through the `validate_raw_data_contract` task in `iot_local_pipeline_dag`.

That task runs before `run_go_producer`.

High-level task flow:

```text
start
-> reset_local_pipeline_state
-> start_infrastructure
-> truncate_warehouse_tables
-> validate_raw_data_contract
-> run_go_producer
-> run_python_consumer
-> run_warehouse_loader
-> remaining pipeline tasks
```

If contract validation fails, the DAG fails early and the producer never starts for that run.

## Dataset Profile Behavior

The validator follows the same dataset-profile selection used by the existing pipeline flow.

Airflow path mapping inside `/opt/project`:

- `sample` -> `/opt/project/data/samples/sample_iot_logs.csv`
- `medium` -> `/opt/project/data/processed/medium_iot_logs.csv`
- `full` -> `/opt/project/data/raw/full_iot_logs.csv`

Behavior by profile:

- `sample` is the default, tracked, lightweight dataset for normal local validation
- `medium` is a generated larger local subset for integration-style runs
- `full` is an intentional heavier raw input path for manual larger-scale validation

## What Happens When Validation Fails

When the validator finds a contract problem, it returns a non-zero exit code and prints a validation summary plus the collected errors.

In practice that means:

- local CLI validation exits with failure
- Airflow marks `validate_raw_data_contract` as failed
- downstream pipeline tasks do not execute in that DAG run
- a local JSON summary can still be written to `docs/data-contract-validation-local.json`

That summary artifact is git-ignored so it can be regenerated safely during local development.

## Validation Layers In Context

Stage 17 introduces one validation layer, but it is not the only quality control in the repository.

### Schema validation

Schema validation checks the expected structure of the raw dataset:

- required columns exist
- non-null required fields are populated
- numeric columns are numeric
- minimum numeric rules are respected
- allowed values stay within the documented contract vocabulary

### Row-level validation

Row-level validation applies those contract checks to actual records in the CSV file. In this project, `scripts/validate_data_contract.py` performs row-level contract checks before the producer runs.

### dbt data quality tests

dbt tests operate later in the pipeline on modeled warehouse data, not on the raw CSV input. They help validate transformed tables, relationships, nullability, uniqueness, and other warehouse-oriented assumptions after ingestion and loading.

### Observability alerts

Observability alerts operate even later and focus on pipeline health signals such as row counts, invalid rates, or other run-level conditions. They help detect operational issues and suspicious outcomes after pipeline execution.

In short:

- data contracts protect the raw input boundary
- row-level validator checks the actual raw records against that contract
- dbt tests protect modeled warehouse data
- observability alerts protect operational pipeline health

## Portfolio Value

Stage 17 demonstrates several practical Data Engineering capabilities that are strong portfolio signals:

- defining a source-level contract instead of relying on implicit assumptions
- building a lightweight validator with only the Python standard library
- integrating validation into orchestration instead of leaving it as a manual-only step
- separating raw schema validation from downstream analytics testing and operational monitoring
- documenting the validation workflow clearly enough for review, onboarding, and future extension

For a portfolio, this shows that the pipeline is not only able to move data, but also able to defend itself against bad or drifting input before expensive downstream work begins.
