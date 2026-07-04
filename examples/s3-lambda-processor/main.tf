terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

variable "provider_endpoint" {
  description = "AWS API endpoint URL for Terraform"
  type        = string
  default     = "http://localhost:8000"
}

variable "lambda_endpoint" {
  description = "AWS API endpoint URL for the Lambda function"
  type        = string
  default     = "http://localhost:8000"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3     = var.provider_endpoint
    lambda = var.provider_endpoint
    iam    = var.provider_endpoint
  }
}

# 1. Create the S3 Bucket
resource "aws_s3_bucket" "upload_bucket" {
  bucket = "incoming-uploads"
}

# 2. Package the Lambda function
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "bootstrap"
  output_path = "function.zip"
}

# 3. Dummy IAM Role (Required by Terraform, ignored by Gopherstack)
resource "aws_iam_role" "lambda_exec" {
  name = "s3-lambda-exec-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# 4. Create the Lambda Function
resource "aws_lambda_function" "processor" {
  function_name    = "s3-processor"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "bootstrap"
  runtime          = "provided.al2"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      AWS_ENDPOINT_URL = var.lambda_endpoint
      AWS_ACCESS_KEY_ID = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION = "us-east-1"
    }
  }
}

# 5. Grant S3 permission to invoke the Lambda
resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processor.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.upload_bucket.arn
}

# 6. Configure the S3 Bucket Notification
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.upload_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.processor.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_bucket]
}
