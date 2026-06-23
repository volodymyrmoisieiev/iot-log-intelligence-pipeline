# AWS Orchestration Foundation

This Terraform root module now covers Stage 19A and Stage 19B of the AWS orchestration foundation for the IoT Log Intelligence Pipeline. It remains intentionally separate from the existing Stage 12 S3 data lake root under `infra/terraform/aws/` so the repository can grow cloud orchestration structure without changing the current local runtime or rewriting the S3 foundation.

## What this module includes

- AWS provider configuration with shared naming and tags
- variable-driven references to an existing or future S3 data lake bucket
- optional IAM execution-role foundations for Lambda and Step Functions
- archive-based packaging for the local metadata-validator Lambda source
- an optional metadata-validator Lambda foundation for file-key validation and layer detection
- optional shared CloudWatch log group foundation
- optional placeholder Step Functions state machine definition for future orchestration work
- safe outputs that expose naming, tags, data lake references, and placeholder definitions

## Cost-safety defaults

All creation toggles default to `false`:

- `create_iam_roles`
- `enable_lambda_foundation`
- `create_cloudwatch_log_group`
- `create_step_function_placeholder`
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

## Recommended relation to Stage 12

Use this root module alongside the existing Stage 12 S3 module:

- `infra/terraform/aws/` remains the storage-focused AWS foundation
- `infra/aws-orchestration/` becomes the control-plane foundation for orchestration and observability

When future stages are ready, wire `existing_data_lake_bucket_name` and `existing_data_lake_bucket_arn` to the real outputs from the Stage 12 S3 root module or from remote state.

## Safe local validation

```powershell
python -m py_compile aws/lambda/iot_metadata_validator/handler.py
python aws/lambda/iot_metadata_validator/handler.py
terraform -chdir=infra/aws-orchestration fmt
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
```

## Future direction

Later stages can extend this foundation with:

- Step Functions tasks that invoke the metadata validator and branch on validation outcomes
- CloudWatch alarms and metrics for orchestration failures and Lambda invocation visibility
- richer event contracts for S3-triggered validation and orchestration handoff
- cross-module wiring to the Stage 12 S3 foundation and future warehouse targets
