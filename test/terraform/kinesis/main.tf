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
    kinesis = var.gopherstack_endpoint
    kms     = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Basic provisioned stream with tags
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "basic" {
  name        = "gopherstack-kinesis-basic"
  shard_count = 2

  retention_period = 48

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Stream with server-side encryption (KMS)
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "encrypted" {
  name        = "gopherstack-kinesis-encrypted"
  shard_count = 1

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = {
    Environment = "test"
    Purpose     = "encryption-test"
  }
}

# ---------------------------------------------------------------------------
# Enhanced fan-out consumer
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream_consumer" "app" {
  name       = "gopherstack-consumer-app"
  stream_arn = aws_kinesis_stream.basic.arn
}

# ---------------------------------------------------------------------------
# On-demand stream (no shard count)
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "ondemand" {
  name = "gopherstack-kinesis-ondemand"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Environment = "test"
    Mode        = "on-demand"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "basic_stream_name" {
  value = aws_kinesis_stream.basic.name
}

output "basic_stream_arn" {
  value = aws_kinesis_stream.basic.arn
}

output "encrypted_stream_arn" {
  value = aws_kinesis_stream.encrypted.arn
}

output "consumer_arn" {
  value = aws_kinesis_stream_consumer.app.arn
}

output "ondemand_stream_arn" {
  value = aws_kinesis_stream.ondemand.arn
}
