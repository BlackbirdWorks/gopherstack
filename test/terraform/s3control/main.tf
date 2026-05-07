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
    s3control = var.gopherstack_endpoint
    s3        = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# S3 bucket referenced by the access point
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "target" {
  bucket = "gopherstack-s3control-target"
}

# ---------------------------------------------------------------------------
# S3 Control Access Point
# ---------------------------------------------------------------------------

resource "aws_s3control_access_point" "example" {
  bucket = aws_s3_bucket.target.id
  name   = "gopherstack-ap"

  public_access_block_configuration {
    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
  }
}

# ---------------------------------------------------------------------------
# Access Point Policy
# ---------------------------------------------------------------------------

resource "aws_s3control_access_point_policy" "example" {
  access_point_arn = aws_s3control_access_point.example.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::000000000000:root" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3control_access_point.example.arn}/object/*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Account-level public access block
# ---------------------------------------------------------------------------

resource "aws_s3control_bucket_public_access_block" "account" {
  account_id              = "000000000000"
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Multi-Region Access Point
# ---------------------------------------------------------------------------

resource "aws_s3control_multi_region_access_point" "example" {
  details {
    name = "gopherstack-mrap"

    region {
      bucket = aws_s3_bucket.target.id
    }
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "access_point_arn" {
  value = aws_s3control_access_point.example.arn
}

output "mrap_alias" {
  value = aws_s3control_multi_region_access_point.example.alias
}
