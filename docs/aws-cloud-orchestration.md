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

Stage 19B now adds the first concrete Lambda foundation: `aws/lambda/iot_metadata_validator/handler.py`. This validator is intentionally simple and standard-library-only. It accepts either an S3-style event or a direct local test payload, extracts bucket and key metadata, validates supported extensions, tries to detect the logical lake layer, and returns structured JSON that a later Step Functions workflow can consume.

### Step Functions orchestration

Step Functions will become the AWS-native orchestration layer for cloud-side flows such as:

- validate inbound object references
- call Lambda preprocessing steps
- branch between success, retry, and failure states
- coordinate future batch or warehouse-loading stages

This complements the current local Airflow orchestration rather than replacing it immediately.

Why Lambda comes before Step Functions in this design:

- it provides a lightweight first-pass validation step before broader orchestration begins
- it keeps simple file metadata rules isolated from the state-machine logic
- it gives future Step Functions branches a clean, structured validation result to act on

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

For the Stage 19B validator Lambda, the IAM policy is intentionally limited to:

- CloudWatch Logs permissions for the Lambda log group
- read-only access to the configured S3 data lake prefixes
- no write permissions to the lake and no broad wildcard admin rights

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

## What Stage 19B implements

Stage 19B extends that foundation with a cost-safe Lambda and IAM layer. It adds:

- a new local Lambda source folder at `aws/lambda/iot_metadata_validator/`
- a metadata-validator `handler.py` that can be tested locally without AWS calls
- a sample S3-style event payload for local smoke tests
- Terraform archive-based packaging for the Lambda source
- a least-privilege Lambda execution role and inline policy
- a Lambda function resource behind `enable_lambda_foundation = false`

This stage still does not require or perform a real AWS deployment.

## Planned Stage 19B, 19C, and 19D scope

### Stage 19C

Stage 19C can connect the new validator foundation to a richer orchestration layer:

- Step Functions invocation of the metadata-validator Lambda
- validation-success and validation-failure branches in the state machine
- CloudWatch metric filters
- CloudWatch alarms
- clearer run-state monitoring and failure notifications for cloud-side orchestration

### Stage 19D

Stage 19D can focus on safer cross-module integration and deployment readiness:

- wiring orchestration inputs to the Stage 12 S3 outputs
- environment-specific tfvars examples
- CI validation expansion for the orchestration root module

## Cost-safety notes

Stage 19A and 19B are designed to stay cheap and safe:

- no credentials are committed
- no account IDs are hardcoded
- no expensive compute or always-on services are introduced
- Terraform files are safe to format, initialize, and validate locally
- real AWS resource creation still requires an intentional future `terraform apply`
- Lambda, Step Functions, IAM, and CloudWatch resource creation are off by default through boolean toggles

## Local Lambda test flow

The Stage 19B validator can be tested locally before any AWS deployment work:

```powershell
python -m py_compile aws/lambda/iot_metadata_validator/handler.py
python aws/lambda/iot_metadata_validator/handler.py
```

The sample event file under `aws/lambda/iot_metadata_validator/sample_event.json` exercises the S3-style event path and prints a structured validation result.

## Why this is portfolio-relevant for Data Engineering

This stage matters for a data engineering portfolio because it shows more than local ETL code. It demonstrates:

- clear separation between data-plane and control-plane design
- readiness for cloud orchestration patterns used in real production platforms
- infrastructure-as-code discipline with safe defaults
- understanding of serverless orchestration, observability, IAM boundaries, event validation, and data lake integration

In practice, this makes the project look closer to a real migration path from local development toward cloud-native data platform architecture.
