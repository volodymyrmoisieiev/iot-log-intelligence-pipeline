# AWS Orchestration Foundation

This Terraform root module now covers Stage 19A, Stage 19B, Stage 19C, and Stage 19D of the AWS orchestration foundation for the IoT Log Intelligence Pipeline. It remains intentionally separate from the existing Stage 12 S3 data lake root under `infra/terraform/aws/` so the repository can grow cloud orchestration structure without changing the current local runtime or rewriting the S3 foundation.

## What this module includes

- AWS provider configuration with shared naming and tags
- variable-driven references to an existing or future S3 data lake bucket
- optional IAM execution-role foundations for Lambda and Step Functions
- archive-based packaging for the local metadata-validator Lambda source
- an optional metadata-validator Lambda foundation for file-key validation and layer detection
- an optional Step Functions orchestration foundation that invokes the metadata-validator Lambda
- optional CloudWatch monitoring foundations for Lambda and Step Functions log groups
- optional CloudWatch alarms for Lambda errors, duration risk, Step Functions failures, and timeout risk
- a readable Terraform-native state machine definition for validate -> decide -> process placeholder -> success/failure
- safe outputs that expose naming, tags, data lake references, and placeholder definitions

## Cost-safety defaults

All creation toggles default to `false`:

- `create_iam_roles`
- `enable_lambda_foundation`
- `enable_step_functions_foundation`
- `enable_cloudwatch_monitoring`
- `enable_cloudwatch_alarms`
- `create_cloudwatch_log_group`
- `enable_step_function_logging`

That means the module can be formatted, initialized, and validated locally without planning a real AWS deployment by default.

## Lambda validator foundation

The Stage 19B Lambda foundation is designed for cloud-side IoT file metadata validation before a future Step Functions workflow starts broader orchestration.

The validator:

- accepts a standard S3-style event or a simple direct test payload
- extracts bucket and object-key metadata when present
- validates supported file types: `.csv`, `.json`, `.jsonl`, `.parquet`
- detects logical layers like `raw`, `processed`, `curated`, and `anomalies`
- returns structured JSON without calling AWS APIs

The Terraform IAM policy for this Lambda stays least-privilege:

- CloudWatch Logs write access is limited to the Lambda log group
- S3 access is read-only for the configured bucket prefixes
- no hardcoded account IDs or credentials are used

Why Lambda comes before Step Functions:

- it provides a fast, cheap first gate for inbound object metadata
- it gives Step Functions a cleaner validated payload to branch on later
- it keeps lightweight validation logic out of the broader orchestration state machine

## Step Functions orchestration foundation

Stage 19C adds a simple orchestration flow that describes the future cloud control-plane pattern:

- `ValidateInputMetadata`
  Step Functions invokes the metadata-validator Lambda with the incoming S3-style payload.
- `CheckValidationResult`
  A Choice state routes valid inputs forward and invalid inputs into a failure path.
- `ProcessingPlaceholder`
  A Pass state marks the future place for Glue, Spark, warehouse loading, or other ETL steps.
- `Success`
  The happy path finishes with a Succeed state.
- `ValidationFailed`
  Invalid metadata ends in a Fail state.

Why Step Functions is useful here:

- it mirrors the intent of the local Airflow DAG at a cloud-control-plane level
- it coordinates validation and future processing without putting all logic into one Lambda
- it provides a clean path for retries, branching, and future AWS ETL orchestration

IAM for this state machine stays least-privilege:

- it can invoke only the metadata-validator Lambda configured for this foundation
- it does not need broad S3 permissions in Stage 19C
- logging remains optional and behind the existing CloudWatch toggle

## CloudWatch monitoring foundation

Stage 19D adds the first monitoring and alarm layer for the cloud orchestration stack.

What gets monitored:

- metadata-validator Lambda errors
- metadata-validator Lambda duration risk
- Step Functions failed executions
- Step Functions timed-out executions
- an optional placeholder alarm for a future custom validation-failure metric

Why CloudWatch matters here:

- it gives the AWS orchestration layer a production-like observability baseline
- it helps surface operational risk early, especially validation failures and timeout pressure
- it prepares the project for future SNS, incident-routing, or dashboard integration without requiring that now

Why alarms stay disabled by default:

- Stage 19D is still a foundation-only phase
- no AWS deploy is required for local validation
- alarms and log groups should be created only when a future stage intentionally enables them

## Recommended relation to Stage 12

Use this root module alongside the existing Stage 12 S3 module:

- `infra/terraform/aws/` remains the storage-focused AWS foundation
- `infra/aws-orchestration/` becomes the control-plane foundation for orchestration and observability

When future stages are ready, wire `existing_data_lake_bucket_name` and `existing_data_lake_bucket_arn` to the real outputs from the Stage 12 S3 root module or from remote state.

## Safe local validation

```powershell
.\.venv-observability\Scripts\python.exe -m py_compile .\aws\lambda\iot_metadata_validator\handler.py
terraform -chdir=infra/aws-orchestration fmt
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
```

Sample Step Functions input for local review lives at `infra/aws-orchestration/examples/step-functions-input.json`.

## Future direction

Later stages can extend this foundation with:

- CloudWatch alarms and metrics for orchestration failures and Lambda invocation visibility
- richer event contracts for S3-triggered validation and orchestration handoff
- replacement of the processing placeholder with concrete AWS ETL and load steps
- SNS or incident-routing integrations for CloudWatch alarms
- cross-module wiring to the Stage 12 S3 foundation and future warehouse targets
