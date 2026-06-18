variable "aws_region" {
  description = "AWS region for future infrastructure resources."
  type        = string
}

variable "project_name" {
  description = "Project name used for tagging and naming future AWS resources."
  type        = string
}

variable "environment" {
  description = "Deployment environment name such as dev or prod."
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Name of the S3 bucket that will back the future AWS data lake."
  type        = string
}

variable "enable_bucket_versioning" {
  description = "Whether to enable versioning for the AWS S3 data lake bucket."
  type        = bool
  default     = true
}
