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
    sns = var.gopherstack_endpoint
    sqs = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SQS queue that receives SNS fan-out
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "fanout" {
  name = "gopherstack-sns-fanout"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# SNS topic
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "main" {
  name = "gopherstack-test-topic"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Topic policy — allow the account to publish and subscribe
# ---------------------------------------------------------------------------

resource "aws_sns_topic_policy" "main" {
  arn = aws_sns_topic.main.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowPublish"
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action    = ["SNS:Publish", "SNS:Subscribe", "SNS:Receive"]
        Resource  = aws_sns_topic.main.arn
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# SQS subscription — wires SNS→SQS fan-out
# ---------------------------------------------------------------------------

resource "aws_sns_topic_subscription" "sqs" {
  topic_arn = aws_sns_topic.main.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.fanout.arn
}

# ---------------------------------------------------------------------------
# Data protection policy on the topic
# ---------------------------------------------------------------------------

resource "aws_sns_topic_data_protection_policy" "main" {
  arn = aws_sns_topic.main.arn

  policy = jsonencode({
    Name        = "GoPherstackDataProtection"
    Description = "Test data protection policy"
    Version     = "2021-06-01"
    Statement   = []
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "topic_arn" {
  value       = aws_sns_topic.main.arn
  description = "ARN of the SNS topic"
}

output "subscription_arn" {
  value       = aws_sns_topic_subscription.sqs.arn
  description = "ARN of the SQS subscription"
}

output "queue_url" {
  value       = aws_sqs_queue.fanout.url
  description = "URL of the fan-out SQS queue"
}

output "queue_arn" {
  value       = aws_sqs_queue.fanout.arn
  description = "ARN of the fan-out SQS queue"
}
