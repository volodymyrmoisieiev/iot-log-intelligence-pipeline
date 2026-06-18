output "aws_region" {
  description = "AWS region configured for this Terraform root module."
  value       = var.aws_region
}

output "data_lake_bucket_name" {
  description = "Name of the AWS S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "ARN of the AWS S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.arn
}

output "data_lake_prefixes" {
  description = "Suggested logical prefixes for future data lake objects."
  value       = local.data_lake_prefixes
}
