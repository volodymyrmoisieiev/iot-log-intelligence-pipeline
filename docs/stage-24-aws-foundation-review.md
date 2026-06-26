# Stage 24 AWS Foundation Review and Deployment Plan

## Scope and current truth

This repository already contains meaningful AWS Terraform foundations, but it does not yet represent a completed or proven AWS deployment.

The current project state is:

- local end-to-end validation is production-like for local development
- AWS foundations exist for future S3, Lambda, Step Functions, IAM, and CloudWatch work
- cloud end-to-end execution is not yet proven in a real AWS account
- no Terraform `apply` is part of this review stage

## AWS foundation that already exists

### 1. Storage foundation under `infra/terraform/aws/`

The Stage 12 storage root already defines a future AWS S3 data lake bucket foundation:

- one `aws_s3_bucket` for the data lake
- S3 bucket versioning controlled by `enable_bucket_versioning` and enabled by default
- default server-side encryption using `AES256`
- public access blocking through `aws_s3_bucket_public_access_block`
- ownership enforcement through `aws_s3_bucket_ownership_controls`
- shared inputs for:
  - `aws_region`
  - `project_name`
  - `environment`
  - `data_lake_bucket_name`
- outputs for:
  - configured AWS region
  - bucket name
  - bucket ARN
  - suggested logical prefixes

The storage root also exposes logical lake prefixes:

- `raw/`
- `processed/`
- `spark/device_features/latest/`

Important limitation: these prefixes are outputs only. The Terraform root does not create S3 objects or placeholder folders for them.

### 2. Orchestration foundation under `infra/aws-orchestration/`

The Stage 19 orchestration root already defines future AWS control-plane building blocks:

- shared naming and tagging locals
- optional IAM execution-role foundations for Lambda and Step Functions
- local archive-based packaging for `aws/lambda/iot_metadata_validator`
- optional metadata-validator Lambda foundation
- optional Step Functions state machine foundation
- optional CloudWatch log groups
- optional CloudWatch alarms
- outputs for names, ARNs, tags, references, state-machine definition, and safety switches

### 3. Lambda foundation already modeled

The Lambda foundation already includes:

- local packaging from the repository source directory
- handler/runtime/memory/timeout/architecture inputs
- environment variables for bucket, project, and environment
- least-privilege read-only S3 access to configured prefixes
- CloudWatch log write permissions

The current Lambda is still a foundation only. It is not deployed unless the toggle is enabled and Terraform is applied in AWS.

### 4. Step Functions foundation already modeled

The Step Functions foundation already includes:

- a validation-first state machine definition
- Lambda invocation of the metadata validator
- success and validation-failure branches
- a `ProcessingPlaceholder` pass state for future ETL steps
- optional execution logging

This is an orchestration scaffold, not a completed cloud workflow. The real processing branch is still a placeholder.

### 5. IAM and CloudWatch foundations already modeled

The orchestration root already includes:

- Lambda execution role and inline policy
- Step Functions execution role and inline policy
- CloudWatch log-group resources for Lambda and Step Functions
- CloudWatch alarms for:
  - Lambda errors
  - Lambda duration risk
  - Step Functions failed executions
  - Step Functions timed-out executions
  - a placeholder validation-failure metric alarm

Alarm actions are currently empty, so the alarm layer is foundation-only and not wired to notification targets.

## What is validated today

### Local validation performed in this review

The following commands were executed safely during this stage:

```powershell
docker compose config
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\run_local_e2e_smoke_test.py
terraform -chdir=infra/terraform/aws init -backend=false
terraform -chdir=infra/terraform/aws validate
git status --short
```

### CI validation currently present

Current GitHub Actions behavior is intentionally validation-only:

- `.github/workflows/terraform-validate.yml` validates `infra/aws-orchestration/`
- it runs:
  - `terraform fmt -check`
  - `terraform init -backend=false`
  - `terraform validate`
- it does not run `terraform plan`
- it does not run `terraform apply`
- it does not require AWS credentials

The broader CI foundation in `.github/workflows/ci.yml` also remains safe:

- it validates repository structure
- it runs `docker compose config`
- it compiles selected Python files with `py_compile`
- it does not deploy infrastructure

### Important CI review finding

The active Terraform GitHub Actions workflow currently targets `infra/aws-orchestration/` only.

That means:

- the orchestration root is actively validated in CI
- the storage root under `infra/terraform/aws/` is documented and locally valid
- the storage root is not currently covered by the active Terraform validation workflow

So the repository does contain AWS Terraform foundations, but active CI coverage is stronger for orchestration than for the S3 storage root.

## What is not deployed yet

Nothing in this review proves a real AWS deployment yet.

The following are not confirmed as deployed in AWS from repository evidence:

- the S3 data lake bucket
- real S3 prefixes or uploaded lake objects
- Lambda function deployment
- Step Functions state machine deployment
- CloudWatch log groups
- CloudWatch alarms
- SNS or other notification targets
- S3 event notifications
- EventBridge triggers
- Glue, EMR, ECS, or warehouse-loading cloud compute
- remote Terraform backend and locking

The current local E2E path remains the proven execution path. The cloud path is still unproven.

## Toggles currently disabled by default

In `infra/aws-orchestration/variables.tf`, the following safety toggles default to `false`:

- `create_iam_roles`
- `create_cloudwatch_log_group`
- `enable_cloudwatch_monitoring`
- `enable_cloudwatch_alarms`
- `enable_validation_failure_alarm_placeholder`
- `enable_step_functions_foundation`
- `enable_step_function_logging`
- `enable_lambda_foundation`

These defaults are the main reason the orchestration root stays cost-safe by default.

## What resources would be created if toggles are enabled

### `create_iam_roles = true`

Would create:

- Lambda execution IAM role
- Lambda inline IAM policy
- Step Functions execution IAM role
- Step Functions inline IAM policy

### `enable_lambda_foundation = true`

Would create:

- local Lambda archive package during Terraform evaluation
- Lambda function
- Lambda IAM role and inline policy if no role ARN override is provided

### `enable_step_functions_foundation = true`

Would create:

- Step Functions state machine
- Step Functions IAM role and inline policy if no role ARN override is provided

### `create_cloudwatch_log_group = true` or `enable_cloudwatch_monitoring = true`

Would create:

- Step Functions log group
- Lambda log group

### `enable_step_function_logging = true`

Would not create a standalone resource by itself, but would attach logging configuration to the Step Functions state machine when:

- Step Functions foundation is enabled
- a log group ARN exists through creation or override

### `enable_cloudwatch_alarms = true`

Would create:

- Lambda error alarm
- Lambda duration-risk alarm
- Step Functions failed-executions alarm when a state machine ARN is available
- Step Functions timed-out-executions alarm when a state machine ARN is available

### `enable_validation_failure_alarm_placeholder = true`

Would additionally create:

- placeholder validation-failure alarm for a future custom metric

### `infra/terraform/aws` with a real `terraform apply`

Would create:

- the S3 data lake bucket
- bucket versioning configuration
- bucket default encryption configuration
- public access block configuration
- ownership controls

It would not create uploaded objects or real folder markers for `raw/`, `processed/`, or `spark/device_features/latest/`.

## Risks before the first AWS apply

### 1. Cloud E2E is not proven yet

The repository has a strong local story, but it does not yet prove real AWS execution from S3 intake through cloud orchestration to downstream results.

### 2. No remote Terraform backend is defined

The reviewed roots use backend-free init for validation. That is safe for CI, but it also means the first real apply needs an explicit decision about:

- remote state storage
- state locking
- team-safe state management

### 3. Active CI does not currently validate the storage root

The active Terraform workflow covers `infra/aws-orchestration/`, not `infra/terraform/aws/`. The S3 storage root was validated locally in this review, but it does not currently have matching active CI coverage.

### 4. Prefix expectations are not fully aligned across the two AWS roots

The storage root exposes:

- `raw/`
- `processed/`
- `spark/device_features/latest/`

The orchestration root defaults to:

- `raw/`
- `processed/`
- `curated/`
- `anomalies/`
- `spark/device_features/latest/`

This is not an apply blocker by itself because these are logical prefixes, not created objects. It is still a design-alignment risk before the first cloud deployment.

### 5. Encryption is secure but minimal

The S3 root enables default encryption with `AES256`. That is valid, but if the target AWS account requires KMS-managed encryption, that change should be made before the first real deployment.

### 6. Monitoring is not wired to response channels

CloudWatch alarms currently have empty `alarm_actions` and `ok_actions`. Even if alarms are created later, they will not notify anyone until SNS or another response target is added.

### 7. The Step Functions processing branch is still a placeholder

Deploying the current state machine would validate workflow scaffolding, not a complete cloud ETL or warehouse-loading flow.

### 8. No AWS event-source integration is present yet

The reviewed foundations do not yet wire:

- S3 event notifications
- EventBridge triggers
- Lambda invoke permissions for S3 or EventBridge starts

That means initial testing will likely be manual rather than event-driven.

## Prerequisites before AWS deployment

Before the first real AWS deployment, the project should have:

- a dedicated AWS account or isolated dev environment
- chosen region and environment naming convention
- a globally unique S3 bucket name
- AWS credentials or SSO profile configured outside the repository
- explicit Terraform backend and state-locking approach
- environment-specific `terraform.tfvars` or equivalent secure input handling
- confirmed tag, retention, and cost-governance expectations
- confirmed decision on `AES256` versus KMS encryption
- aligned logical prefix contract between storage and orchestration roots
- a decision on whether to deploy storage and orchestration from separate states or a controlled wiring strategy
- a minimal manual test plan for Lambda invocation and Step Functions execution

## Recommended safe deployment order

### 1. S3 foundation

Apply only the storage root first:

- create the S3 bucket
- confirm encryption
- confirm versioning
- confirm public access block
- confirm ownership controls
- confirm the final bucket name and outputs

This gives later stages a stable storage anchor without introducing compute or orchestration risk too early.

### 2. IAM minimal roles

Next, enable only the smallest needed IAM execution-role layer:

- create Lambda execution role
- create Step Functions execution role
- review resulting policies in AWS

This keeps least-privilege review separate from compute rollout.

### 3. Lambda foundation

Then deploy the metadata-validator Lambda foundation:

- package the Lambda
- deploy the function
- manually invoke it with sample payloads
- verify CloudWatch log output if monitoring is enabled

This proves packaging, runtime, and handler behavior before state-machine orchestration is added.

### 4. Step Functions foundation

After Lambda works in AWS, deploy the Step Functions foundation:

- create the state machine
- run a manual execution with sample input
- verify the valid and invalid branches

At this point the control-plane pattern can be proven even though the processing step is still a placeholder.

### 5. CloudWatch logs and alarms

Only after compute resources exist, enable the monitoring layer:

- create or wire log groups
- enable Step Functions logging
- create alarms
- review retention and naming

Because notification actions are empty today, treat this as observability setup rather than incident response completion.

### 6. First manual AWS E2E dry run

Run a manual, low-risk AWS dry run:

- upload or reference a test object path
- invoke the validator manually or start the state machine manually
- confirm logs, outputs, failure paths, and permissions
- document exactly what succeeded and what remains placeholder-only

This should be positioned as the first cloud proof-of-foundation, not as a production-ready deployment.

## Proposed Stage 25 scope

Stage 25 should focus on the first controlled AWS storage deployment wave:

- finalize deployment prerequisites for a dev AWS environment
- align Terraform inputs and naming for the first real account
- optionally restore CI validation coverage for `infra/terraform/aws/`
- deploy the S3 foundation only
- capture bucket outputs and post-deploy verification notes

Recommended Stage 25 outcome: proven S3 baseline in AWS, still no cloud orchestration claim yet.

## Proposed Stage 26 scope

Stage 26 should focus on minimal execution foundations:

- deploy minimal IAM roles
- deploy the metadata-validator Lambda foundation
- run manual Lambda payload tests in AWS
- verify CloudWatch log behavior
- keep Step Functions and alarms separate unless Lambda validation is stable

Recommended Stage 26 outcome: proven cloud-side metadata validation in AWS, still not a complete pipeline deployment.

## Proposed Stage 27 scope

Stage 27 should focus on orchestration proof rather than full migration:

- deploy the Step Functions foundation
- enable CloudWatch logging and the initial alarm set
- perform the first manual AWS end-to-end dry run
- document observed behavior, permissions, logs, and known placeholders
- decide what real AWS ETL step should replace `ProcessingPlaceholder`

Recommended Stage 27 outcome: proven AWS orchestration scaffold with manual dry-run evidence, but still not a completed cloud data pipeline.

## Final assessment

The honest status is:

- the repository already contains real AWS foundations
- those foundations are meaningful and reviewable
- they are not the same thing as a completed AWS deployment
- local E2E is the proven path today
- cloud E2E remains the next proof step, not an achieved result
