# mega-batch-2 Terraform integration test
# Tests: EC2 Spot Fleet, DAX, IAM (via Lambda), CloudFormation (via resources), SNS
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
    ec2            = var.gopherstack_endpoint
    iam            = var.gopherstack_endpoint
    lambda         = var.gopherstack_endpoint
    cloudformation = var.gopherstack_endpoint
    sns            = var.gopherstack_endpoint
    kinesis        = var.gopherstack_endpoint
    dynamodb       = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---- IAM Role for Spot Fleet ----

resource "aws_iam_role" "spot_fleet_role" {
  name = "spot-fleet-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "spotfleet.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spot_fleet_policy" {
  role       = aws_iam_role.spot_fleet_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

# ---- EC2 Spot Fleet ----

resource "aws_spot_fleet_request" "main" {
  iam_fleet_role                      = aws_iam_role.spot_fleet_role.arn
  spot_price                          = "0.05"
  target_capacity                     = 2
  allocation_strategy                 = "lowestPrice"
  excess_capacity_termination_policy  = "Default"
  terminate_instances_with_expiration = true

  launch_specification {
    instance_type = "t3.medium"
    ami           = "ami-12345678"
    spot_price    = "0.05"
  }

  tags = {
    Name        = "mega-batch-2-fleet"
    Environment = "test"
  }
}

# ---- EC2 Launch Template ----

resource "aws_launch_template" "main" {
  name          = "mega-batch-2-lt"
  image_id      = "ami-12345678"
  instance_type = "t3.medium"

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name = "mega-batch-2-instance"
    }
  }

  tags = {
    Environment = "test"
  }
}

# ---- IAM Role for Lambda ----

resource "aws_iam_role" "lambda_exec" {
  name = "mega-batch-2-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_logs" {
  name = "lambda-logs-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ---- Lambda Layer ----

resource "aws_lambda_layer_version" "utils" {
  layer_name          = "mega-batch-2-utils"
  description         = "Utility layer for mega-batch-2 tests"
  compatible_runtimes = ["nodejs20.x", "python3.12"]

  # Using a minimal zip for testing
  filename         = "${path.module}/layer.zip"
  source_code_hash = filebase64sha256("${path.module}/layer.zip")
}

# ---- Lambda Function ----

resource "aws_lambda_function" "processor" {
  function_name = "mega-batch-2-processor"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "index.handler"
  runtime       = "nodejs20.x"

  filename         = "${path.module}/function.zip"
  source_code_hash = filebase64sha256("${path.module}/function.zip")

  layers = [aws_lambda_layer_version.utils.arn]

  environment {
    variables = {
      ENVIRONMENT = "test"
    }
  }

  tags = {
    Environment = "test"
  }
}

# ---- SNS Topics ----

resource "aws_sns_topic" "fleet_events" {
  name = "mega-batch-2-fleet-events"

  tags = {
    Environment = "test"
  }
}

resource "aws_sns_topic" "alerts" {
  name = "mega-batch-2-alerts"

  tags = {
    Environment = "test"
  }
}

resource "aws_sns_topic_subscription" "lambda_sub" {
  topic_arn = aws_sns_topic.fleet_events.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.processor.arn
}

# ---- Kinesis Stream ----

resource "aws_kinesis_stream" "events" {
  name             = "mega-batch-2-events"
  shard_count      = 2
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "OutgoingRecords",
    "IncomingRecords",
  ]

  tags = {
    Environment = "test"
  }
}

# ---- DynamoDB Table ----

resource "aws_dynamodb_table" "state" {
  name           = "mega-batch-2-state"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "id"
  range_key      = "timestamp"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "N"
  }

  tags = {
    Environment = "test"
  }
}

# ---- CloudFormation Stack ----

resource "aws_cloudformation_stack" "infra" {
  name = "mega-batch-2-infra"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "mega-batch-2 infrastructure stack"
    Resources = {
      TestQueue = {
        Type = "AWS::SQS::Queue"
        Properties = {
          QueueName = "mega-batch-2-cfn-queue"
        }
      }
    }
    Outputs = {
      QueueUrl = {
        Value       = { Ref = "TestQueue" }
        Description = "URL of the test SQS queue"
      }
    }
  })

  tags = {
    Environment = "test"
  }
}

# ---- Outputs ----

output "spot_fleet_id" {
  description = "Spot Fleet Request ID"
  value       = aws_spot_fleet_request.main.id
}

output "launch_template_id" {
  description = "Launch Template ID"
  value       = aws_launch_template.main.id
}

output "lambda_arn" {
  description = "Lambda Function ARN"
  value       = aws_lambda_function.processor.arn
}

output "layer_arn" {
  description = "Lambda Layer ARN"
  value       = aws_lambda_layer_version.utils.arn
}

output "sns_topic_arn" {
  description = "SNS Fleet Events Topic ARN"
  value       = aws_sns_topic.fleet_events.arn
}

output "kinesis_stream_name" {
  description = "Kinesis Stream Name"
  value       = aws_kinesis_stream.events.name
}

output "dynamodb_table_name" {
  description = "DynamoDB Table Name"
  value       = aws_dynamodb_table.state.name
}

output "cfn_stack_id" {
  description = "CloudFormation Stack ID"
  value       = aws_cloudformation_stack.infra.id
}
