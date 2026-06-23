# AWS Cloud Orchestration Foundation

## Stage 19 purpose

Stage 19 introduces the cloud orchestration layer for the IoT Log Intelligence Pipeline. The goal is not to migrate the current local stack to AWS immediately. Instead, the goal is to prepare a clean, portfolio-ready Terraform and architecture foundation for the control-plane pieces that typically coordinate a modern data platform on AWS.

## Why Stage 19 comes after the local pipeline

The repository already has stable local building blocks for ingestion, validation, warehouse loading, analytics, Airflow orchestration, Spark feature engineering, MinIO-based object storage, observability, data contracts, performance testing, and anomaly detection. Adding AWS orchestration only after those foundations are stable is intentional:

- it keeps the early stages cheap and easy to demo locally
- it avoids coupling infrastructure experiments to unfinished core pipeline logic
- it makes cloud architecture decisions easier because the data flow is already understood
- it creates a cleaner portfolio story: local-first engineering, then cloud-ready orchestration

## Proposed AWS architecture

The planned AWS direction keeps the current local design patterns but maps them to cloud-managed services.

### S3 data lake

S3 remains the central landing zone for raw and curated objects:

- `raw/` for bronze-style inbound log files
- `processed/` for validated or enriched outputs
- `spark/device_features/latest/` for batch feature artifacts and downstream lake use

This aligns with the existing Stage 12 Terraform S3 data lake foundation and the local MinIO workflow already present in the repository.

### Lambda validation and metadata processing

Lambda is a good fit for lightweight serverless tasks such as:

- validating object metadata when new files land in S3
- normalizing manifest files
- writing lightweight run metadata
- preparing orchestration inputs for later processing steps

The intent is to keep Lambda focused on short, event-driven, low-cost logic rather than large transformations.

### Step Functions orchestration

Step Functions will become the AWS-native orchestration layer for cloud-side flows such as:

- validate inbound object references
- call Lambda preprocessing steps
- branch between success, retry, and failure states
- coordinate future batch or warehouse-loading stages

This complements the current local Airflow orchestration rather than replacing it immediately.

### CloudWatch logs and alarms

CloudWatch will provide cloud-native operational visibility:

- centralized execution logs for Step Functions and Lambda
- future alarms for failed executions, retry spikes, or missing data windows
- a foundation for cloud observability alongside the local observability work already in the repo

### IAM least privilege

IAM is designed to stay narrow and explicit:

- separate execution roles for Lambda and Step Functions
- only the minimum S3 and logging permissions needed for future workflow steps
- no hardcoded account IDs, credentials, or broad admin-style policies in the repository

## What Stage 19A implements

Stage 19A is intentionally a foundation-only release. It adds:

- a new Terraform root module at `infra/aws-orchestration/`
- AWS provider configuration and Terraform version constraints
- shared project, environment, naming, and tagging locals
- variable-driven references for the existing or future S3 data lake bucket
- optional IAM role foundations for Lambda and Step Functions
- an optional CloudWatch log group foundation
- an optional placeholder Step Functions state machine definition
- module documentation in `infra/aws-orchestration/README.md`

The defaults are cost-safe: creation toggles are disabled unless a future stage intentionally enables them.

## Planned Stage 19B, 19C, and 19D scope

### Stage 19B

Stage 19B can connect this orchestration foundation to concrete packaging and workflow assets:

- first Lambda handler scaffolding
- Step Functions JSON or ASL workflow refinement
- tighter S3 event and input contracts

### Stage 19C

Stage 19C can strengthen cloud observability:

- CloudWatch metric filters
- CloudWatch alarms
- clearer run-state monitoring and failure notifications

### Stage 19D

Stage 19D can focus on safer cross-module integration and deployment readiness:

- wiring orchestration inputs to the Stage 12 S3 outputs
- environment-specific tfvars examples
- CI validation expansion for the orchestration root module

## Cost-safety notes

Stage 19A is designed to stay cheap and safe:

- no credentials are committed
- no account IDs are hardcoded
- no expensive compute or always-on services are introduced
- Terraform files are safe to format, initialize, and validate locally
- real AWS resource creation still requires an intentional future `terraform apply`
- Step Functions, IAM, and CloudWatch resource creation are off by default through boolean toggles

## Why this is portfolio-relevant for Data Engineering

This stage matters for a data engineering portfolio because it shows more than local ETL code. It demonstrates:

- clear separation between data-plane and control-plane design
- readiness for cloud orchestration patterns used in real production platforms
- infrastructure-as-code discipline with safe defaults
- understanding of serverless orchestration, observability, IAM boundaries, and data lake integration

In practice, this makes the project look closer to a real migration path from local development toward cloud-native data platform architecture.
