# Data Contracts

## Overview

A data contract is a documented agreement about what a dataset is expected to look like before downstream systems depend on it. It usually defines the schema, required fields, expected types, nullability rules, and a small set of baseline constraints.

For this project, the first contract lives in `contracts/iot_raw_log_contract.yml` and covers the raw IoT log CSV used at the front of the pipeline.

## Why This Project Needs Data Contracts

This repository already moves IoT events across several layers: producer, Kafka, consumer validation, warehouse loading, dbt, Airflow, Spark, and observability. Without a shared contract, those layers can silently drift apart when a raw dataset changes shape.

Data contracts help us:

- define the expected raw schema in one place
- reduce ambiguity between ingestion, validation, and downstream modeling
- document which fields are mandatory before later runtime checks are added
- prepare Stage 17B, where automated validation can enforce the contract consistently

Stage 17A intentionally adds documentation and schema definition only. It does not change producer behavior, consumer behavior, warehouse loading, Airflow, dbt, Spark, MinIO, Terraform, or benchmark execution.

## What The Raw IoT Log Contract Validates

The raw contract defines the expected CSV columns for the incoming IoT log dataset:

- `event_timestamp`
- `device_id`
- `source_ip`
- `destination_ip`
- `protocol`
- `packet_size`
- `duration_ms`
- `event_type`
- `attack_type`
- `status`

For each column, the contract documents:

- whether the column is required
- the expected logical type
- whether null values are allowed
- a short description of the field
- a basic validation rule where it is useful

Examples include non-null timestamp and device identifiers, non-negative numeric checks for `packet_size` and `duration_ms`, and initial allowed-value lists for `protocol` and `status`.

## Contract File Location

The contract is stored here:

- `contracts/iot_raw_log_contract.yml`

That file is the Stage 17A source of truth for the raw IoT log CSV schema.

## How Stage 17B Uses The Contract

Stage 17B adds a local validator script at `scripts/validate_data_contract.py`. It reads `contracts/iot_raw_log_contract.yml` and checks a raw CSV file against the documented schema before deeper processing continues.

The local validator can be used to:

- confirm all required columns exist
- verify basic type and nullability expectations
- catch obvious schema breaks earlier in the pipeline
- produce clearer validation errors when the source dataset changes unexpectedly

Stage 17A prepared the contract foundation, and Stage 17B adds the first local enforcement tool without changing producer, consumer, warehouse-loader, Airflow, dbt, Spark, MinIO, Terraform, or benchmark runtime behavior.

## Schema Validation vs Business And Data Quality Validation

Schema validation answers questions like:

- does the file contain all required columns
- is `packet_size` represented as an integer-like value
- are nulls allowed in `attack_type`
- does `status` stay within the currently documented allowed values

Business or data quality validation answers different questions, for example:

- are timestamps realistic for the time period being processed
- do device identifiers map to known devices
- are packet sizes suspiciously large or unexpectedly uniform
- does the ratio of blocked events suggest an upstream issue

In short:

- schema validation checks structure and basic field-level rules
- business/data quality validation checks meaning, realism, and fitness for downstream use

Both matter, but schema validation is the first line of defense and the focus of this Stage 17A foundation.

## Running The Validator

Validate the tracked sample dataset against the default contract:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\validate_data_contract.py --input .\data\samples\sample_iot_logs.csv
```

The validator supports these key arguments:

- `--contract` defaults to `contracts/iot_raw_log_contract.yml`
- `--input` is required and points to the CSV file being checked
- `--max-errors` controls how many validation errors are recorded before the run stops early
- `--summary-json` writes a machine-readable JSON summary report

Allowed-value checks for fields such as `protocol` and `status` are matched case-insensitively so the validator can work with the current raw sample dataset while still enforcing the documented contract vocabulary.

## Writing A JSON Summary

Write a local JSON summary report while validating the sample dataset:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\validate_data_contract.py --input .\data\samples\sample_iot_logs.csv --summary-json docs/data-contract-validation-local.json
```

The JSON summary includes:

- input path
- contract path
- rows checked
- valid rows
- invalid rows
- errors
- timestamp

The local `docs/data-contract-validation-local.json` path is git-ignored so validation evidence can be generated repeatedly without polluting commits.

## Exit Codes

The validator returns:

- `0` when validation passes
- `1` when validation completes but one or more contract checks fail
- `2` when there is a configuration or input problem, such as a missing CSV file, missing contract file, malformed contract structure, or invalid CLI argument values

## How Stage 17C Uses The Contract

Stage 17C wires the Stage 17B validator into `airflow/dags/iot_local_pipeline_dag.py` as a real pre-check task named `validate_raw_data_contract`.

That means Airflow now validates the selected raw CSV before `run_go_producer` starts. If contract validation fails, the DAG stops early and downstream producer, consumer, warehouse-loader, dbt, Spark, MinIO, and observability tasks do not run.

Airflow uses these `/opt/project` path mappings for dataset profiles:

- `sample` -> `/opt/project/data/samples/sample_iot_logs.csv`
- `medium` -> `/opt/project/data/processed/medium_iot_logs.csv`
- `full` -> `/opt/project/data/raw/full_iot_logs.csv`

The Airflow validation task also writes a local summary file to `docs/data-contract-validation-local.json`. That artifact remains git-ignored so it can be regenerated during local orchestration runs without polluting commits.

This Stage 17C integration turns the contract into a practical pipeline guardrail instead of a documentation-only or CLI-only step.
