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
# IAM role (minimal — gopherstack does not enforce IAM policies)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_exec" {
  name = "gopherstack-test-lambda-exec"

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
# Lambda function (container-image based — no zip required)
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "example" {
  function_name = "gopherstack-test-fn"
  package_type  = "Image"
  image_uri     = "public.ecr.aws/lambda/python:3.12"
  role          = aws_iam_role.lambda_exec.arn

  environment {
    variables = {
      ENV = "test"
    }
  }

  tags = {
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Function URL
# ---------------------------------------------------------------------------

resource "aws_lambda_function_url" "example" {
  function_name      = aws_lambda_function.example.function_name
  authorization_type = "NONE"
}

# ---------------------------------------------------------------------------
# Reserved concurrency
# ---------------------------------------------------------------------------

resource "aws_lambda_function_event_invoke_config" "example" {
  function_name                = aws_lambda_function.example.function_name
  maximum_retry_attempts       = 0
  maximum_event_age_in_seconds = 60
}

# ---------------------------------------------------------------------------
# SQS queue for event source mapping
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "trigger" {
  name                       = "gopherstack-test-lambda-trigger"
  visibility_timeout_seconds = 30
}

# ---------------------------------------------------------------------------
# Event source mapping: SQS → Lambda
# ---------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn = aws_sqs_queue.trigger.arn
  function_name    = aws_lambda_function.example.arn
  batch_size       = 10
  enabled          = true
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "function_arn" {
  value       = aws_lambda_function.example.arn
  description = "ARN of the test Lambda function"
}

output "function_url" {
  value       = aws_lambda_function_url.example.function_url
  description = "HTTP endpoint for the function URL"
}

output "esm_uuid" {
  value       = aws_lambda_event_source_mapping.sqs.uuid
  description = "UUID of the event source mapping"
}
