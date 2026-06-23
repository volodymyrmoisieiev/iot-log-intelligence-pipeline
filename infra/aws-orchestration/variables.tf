variable "aws_region" {
  description = "AWS region for the future orchestration stack."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used in AWS resource naming and tagging."
  type        = string
  default     = "iot-log-intelligence-pipeline"
}

variable "environment" {
  description = "Environment label for future deployments such as dev, staging, or prod."
  type        = string
  default     = "dev"
}

variable "additional_tags" {
  description = "Additional tags to merge into the shared AWS tags."
  type        = map(string)
  default     = {}
}

variable "data_lake_bucket_name" {
  description = "Fallback S3 bucket name reference when an existing bucket ARN is not supplied."
  type        = string
  default     = "replace-me-iot-log-intelligence-dev"
}

variable "existing_data_lake_bucket_name" {
  description = "Optional name of the existing Stage 12 S3 data lake bucket to reference from orchestration resources."
  type        = string
  default     = ""
}

variable "existing_data_lake_bucket_arn" {
  description = "Optional ARN of the existing Stage 12 S3 data lake bucket to reference from orchestration resources."
  type        = string
  default     = ""
}

variable "data_lake_prefixes" {
  description = "Logical S3 prefixes that future Lambda and Step Functions orchestration may access."
  type        = list(string)
  default = [
    "raw/",
    "processed/",
    "spark/device_features/latest/",
  ]
}

variable "create_iam_roles" {
  description = "When true, create minimal IAM execution roles for Lambda and Step Functions."
  type        = bool
  default     = false
}

variable "create_cloudwatch_log_group" {
  description = "When true, create a shared CloudWatch log group foundation for orchestration workflows."
  type        = bool
  default     = false
}

variable "cloudwatch_log_retention_days" {
  description = "Retention period for the shared CloudWatch log group if it is created."
  type        = number
  default     = 14
}

variable "cloudwatch_log_group_arn_override" {
  description = "Optional pre-existing CloudWatch log group ARN to use for Step Functions logging instead of creating one."
  type        = string
  default     = ""
}

variable "create_step_function_placeholder" {
  description = "When true, create a minimal Step Functions placeholder state machine for future orchestration stages."
  type        = bool
  default     = false
}

variable "enable_step_function_logging" {
  description = "When true and a log group ARN is available, enable Step Functions execution logging."
  type        = bool
  default     = false
}

variable "step_function_type" {
  description = "Step Functions workflow type for the placeholder state machine."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "EXPRESS"], var.step_function_type)
    error_message = "step_function_type must be STANDARD or EXPRESS."
  }
}

variable "step_functions_role_arn" {
  description = "Optional pre-existing IAM role ARN for the placeholder Step Functions state machine."
  type        = string
  default     = ""
}

variable "lambda_role_name_override" {
  description = "Optional explicit name for the future Lambda execution role."
  type        = string
  default     = ""
}

variable "step_functions_role_name_override" {
  description = "Optional explicit name for the future Step Functions execution role."
  type        = string
  default     = ""
}

variable "cloudwatch_log_group_name_override" {
  description = "Optional explicit name for the shared orchestration CloudWatch log group."
  type        = string
  default     = ""
}

variable "step_function_name_override" {
  description = "Optional explicit name for the placeholder Step Functions state machine."
  type        = string
  default     = ""
}
