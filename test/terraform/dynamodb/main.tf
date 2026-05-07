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
    dynamodb = var.gopherstack_endpoint
    kinesis  = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Primary DynamoDB table with GSI
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "primary" {
  name           = "gopherstack-test-primary"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "pk"
  range_key      = "sk"
  table_class    = "STANDARD"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi_pk"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi-index"
    hash_key        = "gsi_pk"
    projection_type = "ALL"
  }

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Table class switching: STANDARD_INFREQUENT_ACCESS
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "infrequent" {
  name         = "gopherstack-test-infrequent"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  table_class  = "STANDARD_INFREQUENT_ACCESS"

  attribute {
    name = "pk"
    type = "S"
  }

  tags = {
    Environment = "test"
    Purpose     = "table-class-test"
  }
}

# ---------------------------------------------------------------------------
# Kinesis streaming destination
# ---------------------------------------------------------------------------

resource "aws_kinesis_stream" "destination" {
  name        = "gopherstack-ddb-stream"
  shard_count = 1

  tags = {
    Environment = "test"
  }
}

resource "aws_dynamodb_kinesis_streaming_destination" "primary" {
  table_name = aws_dynamodb_table.primary.name
  stream_arn = aws_kinesis_stream.destination.arn
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "primary_table_name" {
  value = aws_dynamodb_table.primary.name
}

output "primary_table_arn" {
  value = aws_dynamodb_table.primary.arn
}

output "infrequent_table_name" {
  value = aws_dynamodb_table.infrequent.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.destination.arn
}
