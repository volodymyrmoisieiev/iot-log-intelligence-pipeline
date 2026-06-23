output "name_prefix" {
  description = "Shared naming prefix for future AWS orchestration resources."
  value       = local.name_prefix
}

output "common_tags" {
  description = "Tags applied to future AWS orchestration resources."
  value       = local.common_tags
}

output "data_lake_reference" {
  description = "Resolved S3 data lake reference used by the orchestration foundation."
  value = {
    bucket_name = local.resolved_data_lake_bucket_name
    bucket_arn  = local.resolved_data_lake_bucket_arn
    prefixes    = var.data_lake_prefixes
  }
}

output "lambda_execution_role_name" {
  description = "Planned metadata-validator Lambda execution role name."
  value       = local.lambda_execution_role_name
}

output "lambda_execution_role_arn" {
  description = "Metadata-validator Lambda execution role ARN when IAM role creation is enabled or an override is supplied."
  value       = local.lambda_function_role_arn
}

output "lambda_function_name" {
  description = "Planned metadata-validator Lambda function name."
  value       = local.lambda_function_name
}

output "lambda_function_arn" {
  description = "Metadata-validator Lambda function ARN when the Lambda foundation is enabled."
  value       = try(aws_lambda_function.metadata_validator[0].arn, null)
}

output "lambda_source_directory" {
  description = "Resolved local source directory for the metadata-validator Lambda package."
  value       = local.lambda_source_directory
}

output "lambda_package_output_path" {
  description = "Planned local archive path for the metadata-validator Lambda package when the foundation is enabled."
  value       = var.enable_lambda_foundation ? data.archive_file.lambda_validator[0].output_path : null
}

output "step_functions_role_name" {
  description = "Planned Step Functions orchestration execution role name."
  value       = local.step_functions_role_name
}

output "step_functions_role_arn" {
  description = "Step Functions orchestration execution role ARN when IAM role creation is enabled or an override is supplied."
  value       = local.step_functions_state_machine_role
}

output "step_functions_target_lambda_arn" {
  description = "Resolved metadata-validator Lambda target used by the Step Functions foundation."
  value       = local.step_functions_lambda_target_arn
}

output "step_function_monitor_target_arn" {
  description = "Resolved Step Functions orchestration ARN used by CloudWatch monitoring and alarms."
  value       = local.step_function_monitor_target_arn
}

output "lambda_log_group_name" {
  description = "CloudWatch log group name for the metadata-validator Lambda."
  value       = local.lambda_log_group_name
}

output "lambda_log_group_arn" {
  description = "CloudWatch log group ARN for the metadata-validator Lambda when monitoring is enabled."
  value       = try(aws_cloudwatch_log_group.lambda_metadata_validator[0].arn, null)
}

output "cloudwatch_log_group_name" {
  description = "Shared CloudWatch log group name for future orchestration workflows."
  value       = local.orchestration_log_group
}

output "cloudwatch_log_group_arn" {
  description = "Shared CloudWatch log group ARN when created or overridden."
  value       = local.step_function_log_group_arn
}

output "lambda_error_alarm_arn" {
  description = "ARN of the metadata-validator Lambda error alarm when alarms are enabled."
  value       = try(aws_cloudwatch_metric_alarm.lambda_errors[0].arn, null)
}

output "lambda_duration_alarm_arn" {
  description = "ARN of the metadata-validator Lambda duration-risk alarm when alarms are enabled."
  value       = try(aws_cloudwatch_metric_alarm.lambda_duration[0].arn, null)
}

output "step_functions_failed_alarm_arn" {
  description = "ARN of the Step Functions failed-executions alarm when alarms are enabled."
  value       = try(aws_cloudwatch_metric_alarm.step_functions_failed[0].arn, null)
}

output "step_functions_timed_out_alarm_arn" {
  description = "ARN of the Step Functions timed-out-executions alarm when alarms are enabled."
  value       = try(aws_cloudwatch_metric_alarm.step_functions_timed_out[0].arn, null)
}

output "validation_failure_placeholder_alarm_arn" {
  description = "ARN of the placeholder validation-failure alarm when that optional alarm is enabled."
  value       = try(aws_cloudwatch_metric_alarm.validation_failure_placeholder[0].arn, null)
}

output "step_function_name" {
  description = "Planned Step Functions orchestration state machine name."
  value       = local.step_function_name
}

output "step_function_definition" {
  description = "JSON definition for the Stage 19C orchestration state machine."
  value       = local.step_function_definition
}

output "step_function_state_machine_arn" {
  description = "Step Functions orchestration state machine ARN when the foundation is enabled."
  value       = try(aws_sfn_state_machine.orchestration[0].arn, null)
}

output "safety_switches" {
  description = "Creation toggles that keep the AWS orchestration foundation cost-safe by default."
  value = {
    create_iam_roles                 = var.create_iam_roles
    create_cloudwatch_log_group      = var.create_cloudwatch_log_group
    enable_cloudwatch_monitoring     = var.enable_cloudwatch_monitoring
    enable_cloudwatch_alarms         = var.enable_cloudwatch_alarms
    enable_lambda_foundation         = var.enable_lambda_foundation
    enable_step_functions_foundation = var.enable_step_functions_foundation
    enable_step_function_logging     = var.enable_step_function_logging
  }
}
