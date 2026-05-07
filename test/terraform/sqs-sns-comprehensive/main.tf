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
# Dead-letter queue (standard) for SNS subscription redrive
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                      = "gopherstack-comprehensive-dlq"
  message_retention_seconds = 86400

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "dlq"
  }
}

# ---------------------------------------------------------------------------
# FIFO queue with content-based deduplication
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "fifo" {
  name                        = "gopherstack-comprehensive-orders.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 30

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Purpose     = "fifo-dedup"
  }
}

# ---------------------------------------------------------------------------
# Standard SQS queue that receives SNS fan-out (with filter policy)
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "fanout" {
  name                      = "gopherstack-comprehensive-fanout"
  message_retention_seconds = 345600

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "fanout"
  }
}

# ---------------------------------------------------------------------------
# Source queue that uses the DLQ as its redrive target
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "source" {
  name                       = "gopherstack-comprehensive-source"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "source"
  }
}

# ---------------------------------------------------------------------------
# SNS topic
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "main" {
  name = "gopherstack-comprehensive-topic"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# SNS → SQS subscription with filter policy
# Only messages with attribute event-type = "order" are delivered.
# ---------------------------------------------------------------------------

resource "aws_sns_topic_subscription" "sqs_filtered" {
  topic_arn = aws_sns_topic.main.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.fanout.arn

  filter_policy = jsonencode({
    "event-type" = ["order"]
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "fifo_queue_url" {
  value       = aws_sqs_queue.fifo.url
  description = "URL of the FIFO queue"
}

output "fifo_queue_arn" {
  value       = aws_sqs_queue.fifo.arn
  description = "ARN of the FIFO queue"
}

output "dlq_url" {
  value       = aws_sqs_queue.dlq.url
  description = "URL of the dead-letter queue"
}

output "dlq_arn" {
  value       = aws_sqs_queue.dlq.arn
  description = "ARN of the dead-letter queue"
}

output "source_queue_url" {
  value       = aws_sqs_queue.source.url
  description = "URL of the source queue with redrive policy"
}

output "fanout_queue_url" {
  value       = aws_sqs_queue.fanout.url
  description = "URL of the SNS fan-out queue"
}

output "topic_arn" {
  value       = aws_sns_topic.main.arn
  description = "ARN of the SNS topic"
}

output "subscription_arn" {
  value       = aws_sns_topic_subscription.sqs_filtered.arn
  description = "ARN of the filtered SQS subscription"
}
