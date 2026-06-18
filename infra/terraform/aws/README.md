# Terraform AWS Foundation

Stage 12A adds an AWS-ready Terraform foundation under `infra/terraform/aws/`.

This stage only prepares project structure, provider configuration, shared variables, and baseline tags for future AWS infrastructure work.

What this stage does:

- defines the Terraform CLI version requirement
- pins the AWS provider version
- configures the AWS provider through `aws_region`
- prepares shared inputs for `project_name` and `environment`
- defines reusable default tags for future resources

What this stage does not do:

- it does not create any AWS resources yet
- it does not create S3 buckets yet
- it does not run `terraform apply`

Local commands:

```bash
cd infra/terraform/aws
terraform fmt
terraform init
terraform validate
terraform plan -var-file="terraform.tfvars.example"
```

Use `terraform.tfvars.example` as a safe reference file and create your own local `terraform.tfvars` only when later stages require real values.

Do not run `terraform apply` in Stage 12A. Real S3 and broader data lake infrastructure will be added in later stages.
