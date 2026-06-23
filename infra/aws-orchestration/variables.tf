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
    "curated/",
    "anomalies/",
    "spark/device_features/latest/",
  ]
}

variable "create_iam_roles" {
  description = "When true, create minimal IAM execution roles for future orchestration resources even if the Lambda function itself stays disabled."
  type        = bool
  default     = false
}

variable "create_cloudwatch_log_group" {
  description = "When true, create a shared CloudWatch log group foundation for orchestration workflows."
  type        = bool
  default     = false
}

variable "enable_cloudwatch_monitoring" {
  description = "When true, create CloudWatch monitoring log-group foundations for Lambda and Step Functions."
  type        = bool
  default     = false
}

variable "enable_cloudwatch_alarms" {
  description = "When true, create CloudWatch alarms for the orchestration foundation."
  type        = bool
  default     = false
}

variable "cloudwatch_log_retention_days" {
  description = "Retention period for the shared CloudWatch log group if it is created."
  type        = number
  default     = 14
}

variable "cloudwatch_alarm_period_seconds" {
  description = "Evaluation period in seconds for CloudWatch alarms in the orchestration foundation."
  type        = number
  default     = 300
}

variable "cloudwatch_alarm_evaluation_periods" {
  description = "Number of evaluation periods used by CloudWatch alarms in the orchestration foundation."
  type        = number
  default     = 1
}

variable "lambda_error_alarm_threshold" {
  description = "Threshold for the Lambda error alarm based on the AWS/Lambda Errors metric."
  type        = number
  default     = 1
}

variable "lambda_duration_alarm_threshold_ms" {
  description = "Threshold in milliseconds for the Lambda duration alarm."
  type        = number
  default     = 25000
}

variable "step_functions_failed_executions_threshold" {
  description = "Threshold for the Step Functions failed executions alarm."
  type        = number
  default     = 1
}

variable "step_functions_timed_out_executions_threshold" {
  description = "Threshold for the Step Functions timed-out executions alarm."
  type        = number
  default     = 1
}

variable "enable_validation_failure_alarm_placeholder" {
  description = "When true, create a placeholder CloudWatch alarm for a future custom validation-failure metric."
  type        = bool
  default     = false
}

variable "validation_failure_alarm_threshold" {
  description = "Threshold for the placeholder validation-failure alarm based on a future custom metric."
  type        = number
  default     = 1
}

variable "cloudwatch_log_group_arn_override" {
  description = "Optional pre-existing CloudWatch log group ARN to use for Step Functions logging instead of creating one."
  type        = string
  default     = ""
}

variable "enable_step_functions_foundation" {
  description = "When true, create the Stage 19C Step Functions orchestration foundation."
  type        = bool
  default     = false
}

variable "enable_step_function_logging" {
  description = "When true and a log group ARN is available, enable Step Functions execution logging."
  type        = bool
  default     = false
}

variable "step_function_type" {
  description = "Step Functions workflow type for the orchestration state machine."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "EXPRESS"], var.step_function_type)
    error_message = "step_function_type must be STANDARD or EXPRESS."
  }
}

variable "step_functions_role_arn" {
  description = "Optional pre-existing IAM role ARN for the Step Functions orchestration state machine."
  type        = string
  default     = ""
}

variable "step_function_state_machine_arn_override" {
  description = "Optional pre-existing ARN for the Step Functions orchestration state machine when the local foundation is disabled."
  type        = string
  default     = ""
}

variable "metadata_validator_lambda_arn_override" {
  description = "Optional pre-existing ARN for the metadata-validator Lambda when the local Lambda foundation is disabled."
  type        = string
  default     = ""
}

variable "lambda_role_name_override" {
  description = "Optional explicit name for the metadata-validator Lambda execution role."
  type        = string
  default     = ""
}

variable "lambda_execution_role_arn_override" {
  description = "Optional pre-existing IAM role ARN for the metadata-validator Lambda function."
  type        = string
  default     = ""
}

variable "enable_lambda_foundation" {
  description = "When true, create the Stage 19B Lambda metadata-validator foundation."
  type        = bool
  default     = false
}

variable "lambda_function_name_override" {
  description = "Optional explicit name for the metadata-validator Lambda function."
  type        = string
  default     = ""
}

variable "lambda_description" {
  description = "Description for the metadata-validator Lambda function."
  type        = string
  default     = "Stage 19B IoT metadata validator foundation."
}

variable "lambda_handler" {
  description = "Handler entrypoint for the metadata-validator Lambda package."
  type        = string
  default     = "handler.lambda_handler"
}

variable "lambda_runtime" {
  description = "Runtime for the metadata-validator Lambda function."
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout_seconds" {
  description = "Timeout in seconds for the metadata-validator Lambda function."
  type        = number
  default     = 30
}

variable "lambda_memory_size_mb" {
  description = "Memory size in MB for the metadata-validator Lambda function."
  type        = number
  default     = 256
}

variable "lambda_architecture" {
  description = "CPU architecture for the metadata-validator Lambda function."
  type        = string
  default     = "x86_64"

  validation {
    condition     = contains(["x86_64", "arm64"], var.lambda_architecture)
    error_message = "lambda_architecture must be x86_64 or arm64."
  }
}

variable "lambda_source_directory_override" {
  description = "Optional override for the local metadata-validator Lambda source directory."
  type        = string
  default     = ""
}

variable "lambda_environment_variables" {
  description = "Additional environment variables for the metadata-validator Lambda function."
  type        = map(string)
  default     = {}
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
  description = "Optional explicit name for the Step Functions orchestration state machine."
  type        = string
  default     = ""
}
