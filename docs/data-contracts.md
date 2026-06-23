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

## How This Will Be Used In Stage 17B

Stage 17B can build a validator that reads the contract and checks incoming raw datasets against it before deeper processing continues.

That later validation step can be used to:

- confirm all required columns exist
- verify basic type and nullability expectations
- catch obvious schema breaks earlier in the pipeline
- produce clearer validation errors when the source dataset changes unexpectedly

Stage 17A only prepares that foundation so Stage 17B can implement enforcement without inventing the rules at runtime.

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
