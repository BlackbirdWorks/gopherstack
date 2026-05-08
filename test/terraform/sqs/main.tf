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
    sqs = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack SQS endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                       = "gopherstack-test-dlq"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 30

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "dlq"
  }
}

# ---------------------------------------------------------------------------
# Source queue with redrive policy pointing to the DLQ
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "source" {
  name                       = "gopherstack-test-source"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600
  delay_seconds              = 0

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
# Redrive allow policy on the DLQ (permits source to use it as DLQ)
# ---------------------------------------------------------------------------

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.source.arn]
  })
}

# ---------------------------------------------------------------------------
# Queue with per-message delay (DelaySeconds on the queue itself)
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "delayed" {
  name          = "gopherstack-test-delayed"
  delay_seconds = 5

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# FIFO queue with ContentBasedDeduplication
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "fifo" {
  name                        = "gopherstack-test-fifo.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 30

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "fifo"
  }
}

# ---------------------------------------------------------------------------
# Queue with KMS encryption (SQS-managed SSE)
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "encrypted" {
  name                      = "gopherstack-test-encrypted"
  sqs_managed_sse_enabled   = true
  visibility_timeout_seconds = 30

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Role        = "encrypted"
  }
}

# ---------------------------------------------------------------------------
# Queue with access policy
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "with_policy" {
  name = "gopherstack-test-policy"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

resource "aws_sqs_queue_policy" "with_policy" {
  queue_url = aws_sqs_queue.with_policy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.with_policy.arn
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "source_queue_url" {
  value       = aws_sqs_queue.source.url
  description = "URL of the source queue"
}

output "source_queue_arn" {
  value       = aws_sqs_queue.source.arn
  description = "ARN of the source queue"
}

output "dlq_url" {
  value       = aws_sqs_queue.dlq.url
  description = "URL of the dead-letter queue"
}

output "dlq_arn" {
  value       = aws_sqs_queue.dlq.arn
  description = "ARN of the dead-letter queue"
}

output "delayed_queue_url" {
  value       = aws_sqs_queue.delayed.url
  description = "URL of the delayed queue"
}

output "fifo_queue_url" {
  value       = aws_sqs_queue.fifo.url
  description = "URL of the FIFO queue"
}

output "fifo_queue_arn" {
  value       = aws_sqs_queue.fifo.arn
  description = "ARN of the FIFO queue"
}

output "encrypted_queue_url" {
  value       = aws_sqs_queue.encrypted.url
  description = "URL of the SSE-encrypted queue"
}

output "policy_queue_url" {
  value       = aws_sqs_queue.with_policy.url
  description = "URL of the queue with access policy"
}

output "policy_queue_arn" {
  value       = aws_sqs_queue.with_policy.arn
  description = "ARN of the queue with access policy"
}
