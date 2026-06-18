output "aws_region" {
  description = "AWS region configured for this Terraform root module."
  value       = var.aws_region
}

output "common_tags" {
  description = "Baseline tags prepared for future AWS resources."
  value       = local.common_tags
}
