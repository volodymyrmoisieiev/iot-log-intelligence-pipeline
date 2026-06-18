# Terraform AWS Data Lake

Stage 12C keeps the Terraform AWS data lake foundation under `infra/terraform/aws/` and adds CI validation for it.

This stage maps the existing local MinIO data lake pattern to a future AWS S3 target. The Terraform root module now defines a secured S3 bucket foundation for future Spark and Parquet workflows while still avoiding any real infrastructure changes until an explicit apply step is chosen later.

What this stage does:

- defines the Terraform CLI version requirement
- pins the AWS provider version
- configures the AWS provider through `aws_region`
- prepares shared inputs for `project_name`, `environment`, and the S3 bucket name
- creates Terraform definitions for an AWS S3 data lake bucket
- enables bucket versioning, default server-side encryption, and public access blocking
- adds GitHub Actions validation for Terraform formatting and configuration syntax
- documents suggested logical prefixes:
  - `raw/`
  - `processed/`
  - `spark/device_features/latest/`

What this stage does not do:

- it does not create resources until `terraform apply` is run
- it does not upload any objects into S3 yet
- it does not add real AWS credentials or secrets to the repository
- it does not change local MinIO, Spark, Airflow, or application services

Local commands:

```bash
cd infra/terraform/aws
terraform fmt
terraform init
terraform validate
```

Run `terraform plan -var-file="terraform.tfvars.example"` only when AWS credentials are configured for a real read/authenticated workflow.

CI validation now runs:

```bash
terraform fmt -check -recursive infra/terraform/aws
cd infra/terraform/aws
terraform init -backend=false
terraform validate
```

Use `terraform.tfvars.example` as a safe reference file and create your own local `terraform.tfvars` only when later stages require real values.

Important notes:

- GitHub Actions does not run `terraform plan` or `terraform apply`.
- AWS credentials are not required for CI validation because initialization uses `-backend=false` and validation is syntax-focused.
- AWS credentials are required only for real `terraform plan` or `terraform apply` workflows against AWS.
- `terraform apply` should not be run yet for this stage.
- The S3 bucket foundation is intended for later Spark and Parquet outputs that currently land in local MinIO.
