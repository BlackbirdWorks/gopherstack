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
    kms = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Basic symmetric KMS key with description and tags
# ---------------------------------------------------------------------------

resource "aws_kms_key" "basic" {
  description             = "gopherstack test key"
  deletion_window_in_days = 7

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Alias pointing at the basic key
# ---------------------------------------------------------------------------

resource "aws_kms_alias" "basic" {
  name          = "alias/gopherstack-test-basic"
  target_key_id = aws_kms_key.basic.key_id
}

# ---------------------------------------------------------------------------
# Key with automatic rotation enabled (365-day period)
# ---------------------------------------------------------------------------

resource "aws_kms_key" "rotating" {
  description             = "gopherstack rotation test key"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "rotating" {
  name          = "alias/gopherstack-test-rotating"
  target_key_id = aws_kms_key.rotating.key_id
}

# ---------------------------------------------------------------------------
# Grant with EncryptionContextSubset constraint
# ---------------------------------------------------------------------------

resource "aws_kms_grant" "subset" {
  name              = "gopherstack-test-grant"
  key_id            = aws_kms_key.basic.key_id
  grantee_principal = "arn:aws:iam::000000000000:role/gopherstack-grant-role"

  operations = ["Decrypt", "Encrypt", "GenerateDataKey"]

  constraints {
    encryption_context_subset = {
      Department = "Engineering"
    }
  }
}

# ---------------------------------------------------------------------------
# Multi-region primary key
# ---------------------------------------------------------------------------

resource "aws_kms_key" "mrk_primary" {
  description             = "gopherstack multi-region primary key"
  multi_region            = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "mrk_primary" {
  name          = "alias/gopherstack-test-mrk"
  target_key_id = aws_kms_key.mrk_primary.key_id
}

# ---------------------------------------------------------------------------
# RSA asymmetric key for signing
# ---------------------------------------------------------------------------

resource "aws_kms_key" "rsa_sign" {
  description              = "gopherstack RSA signing key"
  key_usage                = "SIGN_VERIFY"
  customer_master_key_spec = "RSA_2048"
  deletion_window_in_days  = 7
}

resource "aws_kms_alias" "rsa_sign" {
  name          = "alias/gopherstack-test-rsa-sign"
  target_key_id = aws_kms_key.rsa_sign.key_id
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "basic_key_id" {
  value = aws_kms_key.basic.key_id
}

output "basic_key_arn" {
  value = aws_kms_key.basic.arn
}

output "basic_alias_arn" {
  value = aws_kms_alias.basic.arn
}

output "rotating_key_id" {
  value = aws_kms_key.rotating.key_id
}

output "grant_id" {
  value = aws_kms_grant.subset.grant_id
}

output "mrk_primary_key_id" {
  value = aws_kms_key.mrk_primary.key_id
}

output "rsa_sign_key_id" {
  value = aws_kms_key.rsa_sign.key_id
}
