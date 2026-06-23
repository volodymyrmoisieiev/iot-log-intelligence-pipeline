# Stage 19 AWS Cloud Orchestration

## What Stage 19 adds

Stage 19 adds a cloud orchestration foundation for the IoT Log Intelligence Pipeline. It prepares the repository for future AWS-native workflow coordination, validation, monitoring, and infrastructure growth without changing the current local runtime path.

## Why this is a foundation, not a full AWS migration

Stage 19 does not migrate the local pipeline to AWS yet. It intentionally focuses on safe Terraform structure, Lambda packaging, Step Functions orchestration design, CloudWatch monitoring foundations, IAM least privilege, and documentation. Real deployment still requires an explicit future `terraform apply` with deliberate environment configuration.

## How the local pipeline maps to cloud concepts

- local Airflow DAG maps to AWS Step Functions orchestration for validation-first control-plane workflow management
- local file and object flow maps to S3 data lake layers such as `raw/`, `processed/`, `curated/`, and future analytics-ready outputs
- local validation patterns map to the Lambda metadata validator that inspects inbound file metadata before downstream processing
- local observability patterns map to CloudWatch log groups, metrics, and alarms for cloud-side reliability monitoring

## What is included in Terraform

Stage 19 now includes:

- AWS provider and shared naming/tagging structure in `infra/aws-orchestration/`
- variable-driven S3 data lake references
- Lambda metadata validator packaging and optional Lambda function foundation
- IAM roles and policies for Lambda and Step Functions with least-privilege defaults
- a Terraform-native Step Functions workflow definition for:
  - metadata validation
  - validation branching
  - processing placeholder
  - success/failure outcomes
- optional CloudWatch log-group management
- optional CloudWatch alarms for:
  - Lambda errors
  - Lambda duration risk
  - Step Functions failed executions
  - Step Functions timed-out executions
  - future validation-failure custom metric placeholder

## Cost-safe defaults

Stage 19 is intentionally cost-safe by default:

- no real AWS deployment is required
- no credentials are committed
- no account IDs are hardcoded
- no resources are created unless toggles are explicitly enabled
- no expensive always-on compute services are introduced
- Terraform validation works locally with `init -backend=false`

## Available toggles

- `enable_lambda_foundation = false`
- `enable_step_functions_foundation = false`
- `enable_cloudwatch_monitoring = false`
- `enable_cloudwatch_alarms = false`

Supporting toggles also remain available for IAM role creation, shared CloudWatch log groups, and Step Functions execution logging.

## IAM least privilege

IAM is intentionally narrow:

- Lambda gets CloudWatch Logs write permissions and read-only access to configured S3 prefixes
- Step Functions gets permission to invoke only the metadata-validator Lambda used by this workflow
- CloudWatch monitoring resources do not require broad admin-style permissions in repository code

## What is intentionally not deployed by default

- live AWS Lambda execution
- live Step Functions workflow execution
- SNS notification routing
- S3 event notifications
- EventBridge triggers
- Glue, EMR, ECS, or warehouse-loading compute steps
- production alert destinations

## Local validation

```powershell
docker compose config
terraform -chdir=infra/aws-orchestration fmt
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
.\.venv-observability\Scripts\python.exe -m py_compile .\aws\lambda\iot_metadata_validator\handler.py
```

Useful local review files:

- `aws/lambda/iot_metadata_validator/sample_event.json`
- `infra/aws-orchestration/examples/step-functions-input.json`

## Portfolio value for Data Engineering

This stage demonstrates more than local ETL implementation. It shows:

- cloud-control-plane thinking alongside local pipeline engineering
- infrastructure-as-code discipline with safe defaults
- serverless orchestration design
- least-privilege IAM modeling
- production-like observability planning
- a realistic migration story from local development toward cloud-native data platform architecture

## Future improvements

- real AWS deployment with environment-specific values
- Glue, EMR, or ECS-based processing beyond the current processing placeholder
- EventBridge triggers for orchestration starts
- S3 event notifications wired to the validation flow
- CloudWatch custom metrics for validation outcomes
- CI validation specifically targeting the orchestration Terraform root
