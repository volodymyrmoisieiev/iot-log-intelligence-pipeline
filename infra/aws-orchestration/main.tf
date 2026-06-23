locals {
  common_tags = merge(
    {
      project     = var.project_name
      environment = var.environment
      managed_by  = "terraform"
      stage       = "19C"
      component   = "aws-orchestration"
    },
    var.additional_tags
  )

  name_prefix = join("-", compact([var.project_name, var.environment, "orchestration"]))

  lambda_execution_role_name       = var.lambda_role_name_override != "" ? var.lambda_role_name_override : "${local.name_prefix}-lambda-role"
  lambda_function_name             = var.lambda_function_name_override != "" ? var.lambda_function_name_override : "${local.name_prefix}-metadata-validator"
  step_functions_role_name         = var.step_functions_role_name_override != "" ? var.step_functions_role_name_override : "${local.name_prefix}-step-functions-role"
  orchestration_log_group          = var.cloudwatch_log_group_name_override != "" ? var.cloudwatch_log_group_name_override : "/aws/vendedlogs/states/${local.name_prefix}"
  step_function_name               = var.step_function_name_override != "" ? var.step_function_name_override : "${local.name_prefix}-workflow"
  lambda_source_directory          = var.lambda_source_directory_override != "" ? abspath(var.lambda_source_directory_override) : abspath("${path.module}/../../aws/lambda/iot_metadata_validator")
  lambda_package_output_path       = "${path.module}/.terraform/${local.lambda_function_name}.zip"
  lambda_log_group_name            = "/aws/lambda/${local.lambda_function_name}"
  lambda_role_should_exist         = var.create_iam_roles || var.enable_lambda_foundation
  step_functions_role_should_exist = var.create_iam_roles || var.enable_step_functions_foundation

  resolved_data_lake_bucket_name = var.existing_data_lake_bucket_name != "" ? var.existing_data_lake_bucket_name : var.data_lake_bucket_name
  resolved_data_lake_bucket_arn  = var.existing_data_lake_bucket_arn != "" ? var.existing_data_lake_bucket_arn : "arn:aws:s3:::${local.resolved_data_lake_bucket_name}"
  data_lake_object_arns          = [for prefix in var.data_lake_prefixes : "${local.resolved_data_lake_bucket_arn}/${trim(prefix, "/")}/*"]

  lambda_log_group_arn              = "arn:aws:logs:${var.aws_region}:*:log-group:${local.lambda_log_group_name}"
  lambda_log_stream_arn             = "${local.lambda_log_group_arn}:*"
  lambda_function_role_arn          = var.lambda_execution_role_arn_override != "" ? var.lambda_execution_role_arn_override : try(aws_iam_role.lambda_execution[0].arn, null)
  step_functions_lambda_target_arn  = var.metadata_validator_lambda_arn_override != "" ? var.metadata_validator_lambda_arn_override : try(aws_lambda_function.metadata_validator[0].arn, null)
  step_functions_lambda_target_name = local.step_functions_lambda_target_arn != null ? local.step_functions_lambda_target_arn : local.lambda_function_name
  step_functions_lambda_invoke_resources = local.step_functions_lambda_target_arn != null ? [
    local.step_functions_lambda_target_arn,
    "${local.step_functions_lambda_target_arn}:*",
    ] : [
    "arn:aws:lambda:${var.aws_region}:*:function:${local.lambda_function_name}",
    "arn:aws:lambda:${var.aws_region}:*:function:${local.lambda_function_name}:*",
  ]
  step_function_log_group_arn       = var.cloudwatch_log_group_arn_override != "" ? var.cloudwatch_log_group_arn_override : try(aws_cloudwatch_log_group.orchestration[0].arn, null)
  step_functions_state_machine_role = var.step_functions_role_arn != "" ? var.step_functions_role_arn : try(aws_iam_role.step_functions_execution[0].arn, null)

  step_function_definition_object = {
    Comment = "Stage 19C orchestration foundation for future IoT log cloud workflows."
    StartAt = "ValidateInputMetadata"
    States = {
      ValidateInputMetadata = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = local.step_functions_lambda_target_name
          "Payload.$"  = "$"
        }
        ResultPath = "$.validation_result"
        Next       = "CheckValidationResult"
      }
      CheckValidationResult = {
        Type = "Choice"
        Choices = [
          {
            Variable      = "$.validation_result.Payload.is_valid"
            BooleanEquals = true
            Next          = "ProcessingPlaceholder"
          }
        ]
        Default = "ValidationFailed"
      }
      ProcessingPlaceholder = {
        Type = "Pass"
        Result = {
          status  = "processing_placeholder"
          message = "Future Stage 19D will replace this pass state with AWS ETL, storage, and warehouse orchestration steps."
        }
        ResultPath = "$.processing_result"
        Next       = "Success"
      }
      Success = {
        Type = "Succeed"
      }
      ValidationFailed = {
        Type  = "Fail"
        Error = "MetadataValidationFailed"
        Cause = "The metadata validator Lambda returned is_valid=false."
      }
    }
  }

  step_function_definition = jsonencode(local.step_function_definition_object)
}

data "archive_file" "lambda_validator" {
  count = var.enable_lambda_foundation ? 1 : 0

  type        = "zip"
  source_dir  = local.lambda_source_directory
  output_path = local.lambda_package_output_path
  excludes = [
    "__pycache__",
    "*.pyc",
    "README.md",
    "sample_event.json",
  ]
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "lambda_execution" {
  statement {
    sid    = "AllowLogGroupCreate"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
    ]
    resources = [
      local.lambda_log_group_arn,
    ]
  }

  statement {
    sid    = "AllowCloudWatchLogWrites"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      local.lambda_log_stream_arn,
    ]
  }

  statement {
    sid    = "AllowDataLakeList"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [local.resolved_data_lake_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = var.data_lake_prefixes
    }
  }

  statement {
    sid    = "AllowReadOnlyDataLakeObjectAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAttributes",
      "s3:GetObjectVersion",
    ]
    resources = local.data_lake_object_arns
  }
}

data "aws_iam_policy_document" "step_functions_execution" {
  statement {
    sid    = "AllowCloudWatchLogsWrite"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowMetadataValidatorInvocation"
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction",
    ]
    resources = local.step_functions_lambda_invoke_resources
  }
}

resource "aws_iam_role" "lambda_execution" {
  count = local.lambda_role_should_exist ? 1 : 0

  name               = local.lambda_execution_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = merge(
    local.common_tags,
    {
      name    = local.lambda_execution_role_name
      purpose = "metadata-validator-lambda-execution"
    }
  )
}

resource "aws_iam_role_policy" "lambda_execution" {
  count = local.lambda_role_should_exist ? 1 : 0

  name   = "${local.lambda_execution_role_name}-inline"
  role   = aws_iam_role.lambda_execution[0].id
  policy = data.aws_iam_policy_document.lambda_execution.json
}

resource "aws_iam_role" "step_functions_execution" {
  count = local.step_functions_role_should_exist ? 1 : 0

  name               = local.step_functions_role_name
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json

  tags = merge(
    local.common_tags,
    {
      name    = local.step_functions_role_name
      purpose = "step-functions-orchestration-execution"
    }
  )
}

resource "aws_iam_role_policy" "step_functions_execution" {
  count = local.step_functions_role_should_exist ? 1 : 0

  name   = "${local.step_functions_role_name}-inline"
  role   = aws_iam_role.step_functions_execution[0].id
  policy = data.aws_iam_policy_document.step_functions_execution.json
}

resource "aws_cloudwatch_log_group" "orchestration" {
  count = var.create_cloudwatch_log_group ? 1 : 0

  name              = local.orchestration_log_group
  retention_in_days = var.cloudwatch_log_retention_days

  tags = merge(
    local.common_tags,
    {
      name    = local.orchestration_log_group
      purpose = "future-step-functions-logs"
    }
  )
}

resource "aws_lambda_function" "metadata_validator" {
  count = var.enable_lambda_foundation ? 1 : 0

  function_name = local.lambda_function_name
  description   = var.lambda_description
  role          = local.lambda_function_role_arn
  handler       = var.lambda_handler
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_size_mb
  architectures = [var.lambda_architecture]

  filename         = data.archive_file.lambda_validator[0].output_path
  source_code_hash = data.archive_file.lambda_validator[0].output_base64sha256

  environment {
    variables = merge(
      {
        DATA_LAKE_BUCKET = local.resolved_data_lake_bucket_name
        PROJECT_NAME     = var.project_name
        ENVIRONMENT      = var.environment
      },
      var.lambda_environment_variables
    )
  }

  tags = merge(
    local.common_tags,
    {
      name    = local.lambda_function_name
      purpose = "iot-metadata-validator"
    }
  )

  lifecycle {
    precondition {
      condition     = local.lambda_function_role_arn != null
      error_message = "Enable IAM role creation or provide lambda_execution_role_arn_override before enabling the Lambda foundation."
    }
  }
}

resource "aws_sfn_state_machine" "orchestration" {
  count = var.enable_step_functions_foundation ? 1 : 0

  name       = local.step_function_name
  role_arn   = local.step_functions_state_machine_role
  definition = local.step_function_definition
  type       = var.step_function_type

  dynamic "logging_configuration" {
    for_each = var.enable_step_function_logging && local.step_function_log_group_arn != null ? [1] : []

    content {
      include_execution_data = true
      level                  = "ALL"
      log_destination        = "${local.step_function_log_group_arn}:*"
    }
  }

  tags = merge(
    local.common_tags,
    {
      name    = local.step_function_name
      purpose = "step-functions-orchestration-foundation"
    }
  )

  lifecycle {
    precondition {
      condition     = local.step_functions_state_machine_role != null
      error_message = "Enable IAM role creation or provide step_functions_role_arn before enabling the Step Functions foundation."
    }
  }
}
