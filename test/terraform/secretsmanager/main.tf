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
    secretsmanager = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Basic secret with a string value
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "basic" {
  name        = "gopherstack-test-basic"
  description = "Basic secret for gopherstack testing"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

resource "aws_secretsmanager_secret_version" "basic" {
  secret_id     = aws_secretsmanager_secret.basic.id
  secret_string = jsonencode({ username = "admin", password = "s3cr3t" })
}

# ---------------------------------------------------------------------------
# Secret with binary value
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "binary" {
  name        = "gopherstack-test-binary"
  description = "Binary secret for gopherstack testing"
}

resource "aws_secretsmanager_secret_version" "binary" {
  secret_id     = aws_secretsmanager_secret.binary.id
  secret_binary = base64encode("binary-secret-data")
}

# ---------------------------------------------------------------------------
# Secret with resource-based policy
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "with_policy" {
  name = "gopherstack-test-policy"
}

resource "aws_secretsmanager_secret_version" "with_policy" {
  secret_id     = aws_secretsmanager_secret.with_policy.id
  secret_string = "policy-protected-value"
}

resource "aws_secretsmanager_secret_policy" "example" {
  secret_arn = aws_secretsmanager_secret.with_policy.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::123456789012:root" }
        Action    = "secretsmanager:GetSecretValue"
        Resource  = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Secret rotation (simulated — no real Lambda required)
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "rotated" {
  name = "gopherstack-test-rotated"
}

resource "aws_secretsmanager_secret_version" "rotated_initial" {
  secret_id     = aws_secretsmanager_secret.rotated.id
  secret_string = "initial-secret-value"
}

resource "aws_secretsmanager_secret_rotation" "rotated" {
  secret_id           = aws_secretsmanager_secret.rotated.id
  rotation_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:rotate-secret"

  rotation_rules {
    automatically_after_days = 30
  }

  depends_on = [aws_secretsmanager_secret_version.rotated_initial]
}

# ---------------------------------------------------------------------------
# Multiple secret versions with staging labels
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "versioned" {
  name = "gopherstack-test-versioned"
}

resource "aws_secretsmanager_secret_version" "versioned_v1" {
  secret_id     = aws_secretsmanager_secret.versioned.id
  secret_string = "version-one"
}

resource "aws_secretsmanager_secret_version" "versioned_v2" {
  secret_id     = aws_secretsmanager_secret.versioned.id
  secret_string = "version-two"

  depends_on = [aws_secretsmanager_secret_version.versioned_v1]
}

# ---------------------------------------------------------------------------
# Outputs for verification
# ---------------------------------------------------------------------------

output "basic_secret_arn" {
  value = aws_secretsmanager_secret.basic.arn
}

output "basic_secret_version_id" {
  value = aws_secretsmanager_secret_version.basic.version_id
}

output "rotated_secret_arn" {
  value = aws_secretsmanager_secret.rotated.arn
}

output "versioned_current_version" {
  value = aws_secretsmanager_secret_version.versioned_v2.version_id
}
