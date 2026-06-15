# Architecture

## Local MVP architecture

The local MVP focuses on validating the core data flow with minimal infrastructure. Raw or simulated IoT logs move through a Go producer into Kafka, then into a Python consumer, and finally into a local analytical layer such as DuckDB or a lightweight data lake layout.

From there, dbt and SQL shape the curated data into analytics-ready datasets, and Streamlit exposes the main operational metrics.

## AWS advanced architecture

The AWS design follows the same flow with cloud-native services. Raw data lands in S3 Bronze, Lambda handles lightweight event-driven preprocessing, and Step Functions coordinates multi-step workflows.

Processed outputs move into S3 Silver and Gold before loading into Snowflake or BigQuery for dbt-based modeling. CloudWatch covers logs, monitoring, and operational visibility.
