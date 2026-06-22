provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  
  endpoints {
    lambda         = "http://localhost:8000"
    iam            = "http://localhost:8000"
    stepfunctions  = "http://localhost:8000"
  }
}

# Compress the Lambda source code
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/lambda.zip"
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_exec" {
  name = "sfn_lambda_exec_role"
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

# 1. ValidateOrder Lambda
resource "aws_lambda_function" "validate_order" {
  function_name    = "validate-order"
  handler          = "index.validate"
  runtime          = "nodejs20.x"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
}

# 2. ProcessPayment Lambda
resource "aws_lambda_function" "process_payment" {
  function_name    = "process-payment"
  handler          = "index.processPayment"
  runtime          = "nodejs20.x"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
}

# 3. FulfillOrder Lambda
resource "aws_lambda_function" "fulfill_order" {
  function_name    = "fulfill-order"
  handler          = "index.fulfill"
  runtime          = "nodejs20.x"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
}

# 4. CancelOrder Lambda
resource "aws_lambda_function" "cancel_order" {
  function_name    = "cancel-order"
  handler          = "index.cancel"
  runtime          = "nodejs20.x"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
}

# IAM Role for Step Functions
resource "aws_iam_role" "sfn_exec" {
  name = "sfn_exec_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "order_workflow" {
  name     = "OrderWorkflow"
  role_arn = aws_iam_role.sfn_exec.arn
  
  # Wait for Lambdas to be created first so their ARNs resolve in the ASL, 
  # or in Gopherstack's case it might not matter, but good practice.
  depends_on = [
    aws_lambda_function.validate_order,
    aws_lambda_function.process_payment,
    aws_lambda_function.fulfill_order,
    aws_lambda_function.cancel_order
  ]

  definition = file("${path.module}/state-machine.asl.json")
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.order_workflow.arn
}
