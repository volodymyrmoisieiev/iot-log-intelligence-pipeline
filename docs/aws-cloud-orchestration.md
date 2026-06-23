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

Stage 19C adds the first real state-machine foundation for that flow. The workflow is intentionally small and readable:

- `ValidateInputMetadata` invokes the metadata-validator Lambda
- `CheckValidationResult` branches on `is_valid`
- `ProcessingPlaceholder` reserves the next ETL slot for future AWS work
- `Success` ends the happy path
- `ValidationFailed` ends the invalid-input path

This mirrors the local Airflow DAG concept at a cloud-control-plane level: validate first, branch on outcome, then continue to processing only when the input is safe enough to move forward.

Why Lambda comes before Step Functions in this design:

- it provides a lightweight first-pass validation step before broader orchestration begins
- it keeps simple file metadata rules isolated from the state-machine logic
- it gives future Step Functions branches a clean, structured validation result to act on

Why Step Functions is useful here:

- it coordinates multiple cloud-side steps without forcing all orchestration into a single Lambda
- it prepares a clean bridge toward future Glue, Spark, warehouse, or lakehouse tasks
- it provides a natural AWS analogue to the orchestration role that Airflow plays locally

### CloudWatch logs and alarms

CloudWatch will provide cloud-native operational visibility:

- centralized execution logs for Step Functions and Lambda
- future alarms for failed executions, retry spikes, or missing data windows
- a foundation for cloud observability alongside the local observability work already in the repo

Stage 19D turns that into a concrete monitoring foundation. It adds optional log-group management plus optional alarms for:

- Lambda `Errors`
- Lambda `Duration` as timeout-risk protection
- Step Functions `ExecutionsFailed`
- Step Functions `ExecutionsTimedOut`
- a placeholder custom validation-failure alarm for a future emitted metric

This is intentionally production-minded but still cost-safe: monitoring and alarms remain disabled by default until a future deployment stage explicitly enables them.

### IAM least privilege

IAM is designed to stay narrow and explicit:

- separate execution roles for Lambda and Step Functions
- only the minimum S3 and logging permissions needed for future workflow steps
- no hardcoded account IDs, credentials, or broad admin-style policies in the repository

For the Stage 19B validator Lambda, the IAM policy is intentionally limited to:

- CloudWatch Logs permissions for the Lambda log group
- read-only access to the configured S3 data lake prefixes
- no write permissions to the lake and no broad wildcard admin rights

For the Stage 19C Step Functions foundation, the IAM policy is intentionally limited to:

- Step Functions logging-related permissions only when logging is enabled
- Lambda invocation permissions only for the metadata-validator Lambda used in this workflow
- no broad S3 write access and no hardcoded account-specific identities

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

## What Stage 19C implements

Stage 19C extends the AWS control-plane foundation with:

- a Terraform-native Step Functions state machine definition
- a validation-first orchestration path from S3-style input to success/failure
- least-privilege Step Functions IAM for invoking the metadata-validator Lambda
- an example Step Functions input payload at `infra/aws-orchestration/examples/step-functions-input.json`
- a cost-safe state machine resource behind `enable_step_functions_foundation = false`

This stage still does not require or perform a real AWS deployment.

## What Stage 19D implements

Stage 19D extends that control-plane foundation with:

- optional CloudWatch log groups for the metadata-validator Lambda and Step Functions workflow
- optional CloudWatch alarms for Lambda errors and duration risk
- optional CloudWatch alarms for Step Functions failed and timed-out executions
- an optional placeholder validation-failure alarm for a future custom metric
- safe Terraform outputs for log groups and alarm ARNs when resources are enabled

This stage still does not require or perform a real AWS deployment.

## Planned Stage 19E scope

### Stage 19E

Stage 19E can focus on safer cross-module integration and deployment readiness:

- wiring orchestration inputs to the Stage 12 S3 outputs
- replacing the processing placeholder with concrete ETL, storage, or warehouse steps
- connecting CloudWatch alarms to SNS, incident-routing, or richer dashboards
- environment-specific tfvars examples
- CI validation expansion for the orchestration root module

## Cost-safety notes

Stage 19A, 19B, 19C, and 19D are designed to stay cheap and safe:

- no credentials are committed
- no account IDs are hardcoded
- no expensive compute or always-on services are introduced
- Terraform files are safe to format, initialize, and validate locally
- real AWS resource creation still requires an intentional future `terraform apply`
- Lambda, Step Functions, IAM, and CloudWatch resource creation are off by default through boolean toggles

## Local validation flow

The Stage 19B validator syntax can still be validated locally before any AWS deployment work:

```powershell
.\.venv-observability\Scripts\python.exe -m py_compile .\aws\lambda\iot_metadata_validator\handler.py
terraform -chdir=infra/aws-orchestration fmt
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
```

The sample event file under `aws/lambda/iot_metadata_validator/sample_event.json` and the Step Functions example input under `infra/aws-orchestration/examples/step-functions-input.json` give future stages a shared S3-style payload shape for local review and testing.

## Why this is portfolio-relevant for Data Engineering

This stage matters for a data engineering portfolio because it shows more than local ETL code. It demonstrates:

- clear separation between data-plane and control-plane design
- readiness for cloud orchestration patterns used in real production platforms
- infrastructure-as-code discipline with safe defaults
- understanding of serverless orchestration, observability, IAM boundaries, event validation, branching workflow design, and data lake integration

In practice, this makes the project look closer to a real migration path from local development toward cloud-native data platform architecture.
