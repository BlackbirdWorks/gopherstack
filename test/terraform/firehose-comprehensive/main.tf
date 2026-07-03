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
# 5. Extended S3 stream with Parquet data-format conversion
#    (mirrors terraform-provider-aws extendedS3DataFormatConversion coverage)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "firehose" {
  name = "firehose_comprehensive_db"
}

resource "aws_kinesis_firehose_delivery_stream" "s3_parquet" {
  name        = "firehose-comprehensive-parquet"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.primary.arn
    buffering_size      = 128
    error_output_prefix = "conversion-errors/"

    data_format_conversion_configuration {
      enabled = true

      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }

      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }

      schema_configuration {
        database_name = aws_glue_catalog_database.firehose.name
        role_arn      = aws_iam_role.firehose.arn
        table_name    = "events"
      }
    }
  }

  tags = {
    Environment = "test"
    Purpose     = "data-format-conversion"
  }
}

# ---------------------------------------------------------------------------
# 6. Extended S3 stream with dynamic partitioning by a jq query key
#    (mirrors terraform-provider-aws dynamicPartitioning coverage)
# ---------------------------------------------------------------------------

resource "aws_kinesis_firehose_delivery_stream" "s3_dynamic_partitioning" {
  name        = "firehose-comprehensive-dynpart"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.primary.arn
    buffering_size      = 64
    buffering_interval  = 60
    prefix              = "data/customer=!{partitionKeyFromQuery:customer_id}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/"

    dynamic_partitioning_configuration {
      enabled = true

      retry_options {
        duration_in_seconds = 300
      }
    }

    processing_configuration {
      enabled = true

      processors {
        type = "MetadataExtraction"

        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{customer_id:.customer_id}"
        }

        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }
  }

  tags = {
    Environment = "test"
    Purpose     = "dynamic-partitioning"
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

output "s3_parquet_stream_arn" {
  description = "ARN of the Parquet data-format-conversion delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_parquet.arn
}

output "s3_dynamic_partitioning_stream_arn" {
  description = "ARN of the dynamic-partitioning delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_dynamic_partitioning.arn
}
