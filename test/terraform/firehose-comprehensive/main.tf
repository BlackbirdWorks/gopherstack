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
    firehose = var.gopherstack_endpoint
    iam      = var.gopherstack_endpoint
    kms      = var.gopherstack_endpoint
    lambda   = var.gopherstack_endpoint
    s3       = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "GopherStack local endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Common IAM role for Firehose
# ---------------------------------------------------------------------------

resource "aws_iam_role" "firehose" {
  name = "firehose-comprehensive-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ---------------------------------------------------------------------------
# S3 buckets for delivery destinations
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "primary" {
  bucket = "firehose-comprehensive-primary"
}

resource "aws_s3_bucket" "backup" {
  bucket = "firehose-comprehensive-backup"
}

# ---------------------------------------------------------------------------
# 1. Extended S3 delivery stream with GZIP compression and transformation
#    (ProcessingConfiguration with a placeholder Lambda ARN)
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "s3_compressed" {
  name        = "firehose-comprehensive-s3-compressed"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = aws_s3_bucket.primary.arn
    compression_format = "GZIP"
    prefix             = "data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/"
    error_output_prefix = "errors/year=!{timestamp:yyyy}/month=!{timestamp:MM}/!{firehose:error-output-type}/"

    buffering_size     = 10
    buffering_interval = 60

    processing_configuration {
      enabled = true

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "arn:aws:lambda:us-east-1:000000000000:function:firehose-transformer"
        }

        parameters {
          parameter_name  = "NumberOfRetries"
          parameter_value = "3"
        }
      }
    }
  }

  tags = {
    Environment = "test"
    Purpose     = "compression-transformation"
  }
}

# ---------------------------------------------------------------------------
# 2. Extended S3 stream with S3 backup enabled
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "s3_with_backup" {
  name        = "firehose-comprehensive-s3-backup"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = aws_s3_bucket.primary.arn
    compression_format = "UNCOMPRESSED"
    s3_backup_mode     = "Enabled"

    s3_backup_configuration {
      role_arn           = aws_iam_role.firehose.arn
      bucket_arn         = aws_s3_bucket.backup.arn
      compression_format = "GZIP"
      prefix             = "backup/"
    }
  }

  tags = {
    Environment = "test"
    Purpose     = "backup-enabled"
  }
}

# ---------------------------------------------------------------------------
# 3. HTTP endpoint destination stream
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "http_endpoint" {
  name        = "firehose-comprehensive-http-endpoint"
  destination = "http_endpoint"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  http_endpoint_configuration {
    url                = "https://example-endpoint.datadog.com/api/v1/input"
    name               = "Example HTTP Endpoint"
    access_key         = "supersecretaccesskey"
    buffering_size     = 5
    buffering_interval = 300
    role_arn           = aws_iam_role.firehose.arn
    s3_backup_mode     = "FailedDataOnly"

    s3_configuration {
      role_arn           = aws_iam_role.firehose.arn
      bucket_arn         = aws_s3_bucket.backup.arn
      compression_format = "GZIP"
    }

    request_configuration {
      content_encoding = "GZIP"
    }
  }

  tags = {
    Environment = "test"
    Purpose     = "http-endpoint"
  }
}

# ---------------------------------------------------------------------------
# 4. Basic S3 stream — then encrypt it via a separate resource
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "encrypted" {
  name        = "firehose-comprehensive-encrypted"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.primary.arn
  }

  server_side_encryption {
    enabled  = true
    key_type = "AWS_OWNED_CMK"
  }

  tags = {
    Environment = "test"
    Purpose     = "encryption"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "s3_compressed_stream_arn" {
  description = "ARN of the GZIP-compressed S3 delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_compressed.arn
}

output "s3_compressed_stream_name" {
  description = "Name of the GZIP-compressed S3 delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_compressed.name
}

output "s3_backup_stream_arn" {
  description = "ARN of the S3 stream with backup enabled"
  value       = aws_kinesis_firehose_delivery_stream.s3_with_backup.arn
}

output "http_endpoint_stream_arn" {
  description = "ARN of the HTTP endpoint delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.http_endpoint.arn
}

output "encrypted_stream_arn" {
  description = "ARN of the server-side encrypted delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.encrypted.arn
}
