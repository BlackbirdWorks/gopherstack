terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    lambda = var.gopherstack_endpoint
    iam    = var.gopherstack_endpoint
    sqs    = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_exec" {
  name = "gopherstack-comprehensive-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ---------------------------------------------------------------------------
# Code signing config
# ---------------------------------------------------------------------------

resource "aws_lambda_code_signing_config" "example" {
  description = "Gopherstack comprehensive test code signing config"

  allowed_publishers {
    signing_profile_version_arns = [
      "arn:aws:signer:us-east-1:000000000000:/signing-profiles/test-profile/abcdefgh",
    ]
  }

  policies {
    untrusted_artifact_on_deployment = "Warn"
  }
}

# ---------------------------------------------------------------------------
# Lambda function
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "main" {
  function_name = "gopherstack-comprehensive-fn"
  package_type  = "Image"
  image_uri     = "public.ecr.aws/lambda/python:3.12"
  role          = aws_iam_role.lambda_exec.arn

  architectures = ["x86_64"]

  environment {
    variables = {
      STAGE = "test"
    }
  }

  tags = {
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Attach code signing config to function
# ---------------------------------------------------------------------------

resource "aws_lambda_function_code_signing_config" "main" {
  function_name         = aws_lambda_function.main.function_name
  code_signing_config_arn = aws_lambda_code_signing_config.example.arn
}

# ---------------------------------------------------------------------------
# Publish a version so we can create an alias
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "versioned" {
  function_name = "gopherstack-comprehensive-versioned"
  package_type  = "Image"
  image_uri     = "public.ecr.aws/lambda/python:3.12"
  role          = aws_iam_role.lambda_exec.arn
  publish       = true

  architectures = ["arm64"]
}

# ---------------------------------------------------------------------------
# Function alias pointing at $LATEST (version string)
# ---------------------------------------------------------------------------

resource "aws_lambda_alias" "live" {
  name             = "live"
  description      = "Live traffic alias"
  function_name    = aws_lambda_function.versioned.function_name
  function_version = aws_lambda_function.versioned.version
}

# ---------------------------------------------------------------------------
# Provisioned concurrency for the alias
# ---------------------------------------------------------------------------

resource "aws_lambda_provisioned_concurrency_config" "live" {
  function_name                  = aws_lambda_alias.live.function_name
  qualifier                      = aws_lambda_alias.live.name
  provisioned_concurrent_executions = 2
}

# ---------------------------------------------------------------------------
# Async invocation config with destinations
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "on_failure" {
  name = "gopherstack-comprehensive-on-failure"
}

resource "aws_lambda_function_event_invoke_config" "main" {
  function_name          = aws_lambda_function.main.function_name
  maximum_retry_attempts = 1

  destination_config {
    on_failure {
      destination = aws_sqs_queue.on_failure.arn
    }
  }
}

# ---------------------------------------------------------------------------
# SQS queue for event source mapping
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "trigger" {
  name                       = "gopherstack-comprehensive-trigger"
  visibility_timeout_seconds = 30
}

resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn = aws_sqs_queue.trigger.arn
  function_name    = aws_lambda_function.main.arn
  batch_size       = 5
  enabled          = true
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "function_arn" {
  value       = aws_lambda_function.main.arn
  description = "ARN of the main test Lambda function"
}

output "versioned_function_arn" {
  value       = aws_lambda_function.versioned.arn
  description = "ARN of the versioned Lambda function"
}

output "alias_arn" {
  value       = aws_lambda_alias.live.arn
  description = "ARN of the live alias"
}

output "code_signing_config_arn" {
  value       = aws_lambda_code_signing_config.example.arn
  description = "ARN of the code signing config"
}

output "esm_uuid" {
  value       = aws_lambda_event_source_mapping.sqs.uuid
  description = "UUID of the SQS event source mapping"
}
