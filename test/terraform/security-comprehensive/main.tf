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
    kms            = var.gopherstack_endpoint
    acm            = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Secrets Manager — secret with cross-region replication
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "replicated" {
  name        = "security-comprehensive-replicated-secret"
  description = "A secret with cross-region replication for testing"

  replica {
    region = "us-west-2"
  }

  tags = {
    Environment = "test"
    Component   = "security-comprehensive"
  }
}

resource "aws_secretsmanager_secret_version" "replicated" {
  secret_id     = aws_secretsmanager_secret.replicated.id
  secret_string = jsonencode({ username = "admin", password = "s3cr3t!" })
}

# ---------------------------------------------------------------------------
# Secrets Manager — secret with automatic rotation rules
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "rotated" {
  name        = "security-comprehensive-rotated-secret"
  description = "A secret configured for automatic rotation"

  tags = {
    Environment = "test"
    Component   = "security-comprehensive"
  }
}

resource "aws_secretsmanager_secret_version" "rotated" {
  secret_id     = aws_secretsmanager_secret.rotated.id
  secret_string = "initial-value"
}

resource "aws_secretsmanager_secret_rotation" "rotated" {
  secret_id           = aws_secretsmanager_secret.rotated.id
  rotation_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:fake-rotation-lambda"

  rotation_rules {
    automatically_after_days = 30
  }
}

# ---------------------------------------------------------------------------
# KMS — symmetric key with alias (key import simulation)
# ---------------------------------------------------------------------------

resource "aws_kms_key" "import_key" {
  description             = "KMS key with EXTERNAL origin for key material import"
  deletion_window_in_days = 7
  is_enabled              = true

  tags = {
    Environment = "test"
    Component   = "security-comprehensive"
  }
}

resource "aws_kms_alias" "import_key" {
  name          = "alias/security-comprehensive-import-key"
  target_key_id = aws_kms_key.import_key.key_id
}

# ---------------------------------------------------------------------------
# KMS — standard symmetric key used with a custom key store (simulated)
# ---------------------------------------------------------------------------

resource "aws_kms_key" "standard" {
  description             = "Standard symmetric KMS key for security-comprehensive test"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Environment = "test"
    Component   = "security-comprehensive"
  }
}

resource "aws_kms_alias" "standard" {
  name          = "alias/security-comprehensive-standard"
  target_key_id = aws_kms_key.standard.key_id
}

# ---------------------------------------------------------------------------
# ACM — certificate request (simulates Amazon-issued certificate)
# ---------------------------------------------------------------------------

resource "aws_acm_certificate" "example" {
  domain_name       = "security-comprehensive.example.com"
  validation_method = "DNS"

  subject_alternative_names = [
    "www.security-comprehensive.example.com",
    "api.security-comprehensive.example.com",
  ]

  tags = {
    Environment = "test"
    Component   = "security-comprehensive"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "replicated_secret_arn" {
  description = "ARN of the replicated secret"
  value       = aws_secretsmanager_secret.replicated.arn
}

output "rotated_secret_arn" {
  description = "ARN of the rotated secret"
  value       = aws_secretsmanager_secret.rotated.arn
}

output "import_key_id" {
  description = "KMS key ID for the import key"
  value       = aws_kms_key.import_key.key_id
}

output "import_key_arn" {
  description = "KMS key ARN for the import key"
  value       = aws_kms_key.import_key.arn
}

output "standard_key_id" {
  description = "KMS key ID for the standard key"
  value       = aws_kms_key.standard.key_id
}

output "acm_certificate_arn" {
  description = "ACM certificate ARN"
  value       = aws_acm_certificate.example.arn
}

output "acm_domain_validation_options" {
  description = "ACM domain validation options"
  value       = aws_acm_certificate.example.domain_validation_options
}
