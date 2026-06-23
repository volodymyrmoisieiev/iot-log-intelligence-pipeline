# AWS Orchestration Foundation

This Terraform root module adds the Stage 19A AWS orchestration foundation for the IoT Log Intelligence Pipeline. It is intentionally separate from the existing Stage 12 S3 data lake root under `infra/terraform/aws/` so the repository can grow cloud orchestration structure without changing the current local runtime or rewriting the S3 foundation.

## What this module includes

- AWS provider configuration with shared naming and tags
- variable-driven references to an existing or future S3 data lake bucket
- optional IAM execution-role foundations for Lambda and Step Functions
- optional shared CloudWatch log group foundation
- optional placeholder Step Functions state machine definition for future orchestration work
- safe outputs that expose naming, tags, data lake references, and placeholder definitions

## Cost-safety defaults

All creation toggles default to `false`:

- `create_iam_roles`
- `create_cloudwatch_log_group`
- `create_step_function_placeholder`
- `enable_step_function_logging`

That means Stage 19A can be formatted, initialized, and validated locally without planning a real AWS deployment by default.

## Recommended relation to Stage 12

Use this root module alongside the existing Stage 12 S3 module:

- `infra/terraform/aws/` remains the storage-focused AWS foundation
- `infra/aws-orchestration/` becomes the control-plane foundation for orchestration and observability

When future stages are ready, wire `existing_data_lake_bucket_name` and `existing_data_lake_bucket_arn` to the real outputs from the Stage 12 S3 root module or from remote state.

## Safe local validation

```powershell
terraform -chdir=infra/aws-orchestration fmt
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
```

## Future direction

Later stages can extend this foundation with:

- Lambda packaging and handler-specific IAM policies
- Step Functions tasks that call Lambda, Glue, or data-load steps
- CloudWatch alarms and metrics for orchestration failures
- cross-module wiring to the Stage 12 S3 foundation and future warehouse targets
