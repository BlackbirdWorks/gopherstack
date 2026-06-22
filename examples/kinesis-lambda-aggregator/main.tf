provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  
  endpoints {
    lambda     = "http://localhost:8000"
    iam        = "http://localhost:8000"
    dynamodb   = "http://localhost:8000"
    kinesis    = "http://localhost:8000"
  }
}

resource "null_resource" "dynamodb_table" {
  provisioner "local-exec" {
    command = "AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url http://localhost:8000 --region us-east-1 dynamodb create-table --table-name event-metrics --attribute-definitions AttributeName=eventType,AttributeType=S --key-schema AttributeName=eventType,KeyType=HASH --billing-mode PAY_PER_REQUEST || true"
  }
}

# Kinesis Stream
resource "aws_kinesis_stream" "events" {
  name             = "event-stream"
  shard_count      = 1
  retention_period = 24
}

# Compress the Lambda source code
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/lambda.zip"
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_exec" {
  name = "kinesis_lambda_exec_role"
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

# Lambda Function
resource "aws_lambda_function" "processor" {
  function_name    = "event-processor"
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10

  environment {
    variables = {
      TABLE_NAME = "event-metrics"
    }
  }
}

# Event Source Mapping (Connect Kinesis to Lambda)
resource "aws_lambda_event_source_mapping" "kinesis_mapping" {
  event_source_arn  = aws_kinesis_stream.events.arn
  function_name     = aws_lambda_function.processor.arn
  starting_position = "TRIM_HORIZON"
  batch_size        = 100
}

output "stream_name" {
  value = aws_kinesis_stream.events.name
}

output "table_name" {
  value = "event-metrics"
  depends_on = [null_resource.dynamodb_table]
}
