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
