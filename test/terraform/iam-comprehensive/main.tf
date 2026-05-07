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
    iam = var.gopherstack_endpoint
    sts = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# OIDC provider (used for web-identity trust policy)
# ---------------------------------------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = [
    "sts.amazonaws.com",
  ]

  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
  ]

  tags = {
    Provider = "github-actions"
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Role with OIDC trust policy (for AssumeRoleWithWebIdentity)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "oidc_role" {
  name = "gopherstack-iam-comprehensive-oidc-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = {
    Environment = "test"
    UseCase     = "oidc-web-identity"
  }
}

# ---------------------------------------------------------------------------
# Managed policy with tags
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "read_only" {
  name        = "gopherstack-iam-comprehensive-read-only"
  description = "Read-only access policy for comprehensive IAM test"
  path        = "/comprehensive/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = ["*"]
    }]
  })

  tags = {
    Environment = "test"
    PolicyType  = "read-only"
    ManagedBy   = "terraform"
  }
}

# Attach the managed policy to the OIDC role
resource "aws_iam_role_policy_attachment" "oidc_role_policy" {
  role       = aws_iam_role.oidc_role.name
  policy_arn = aws_iam_policy.read_only.arn
}

# ---------------------------------------------------------------------------
# IAM user for MFA and SSH key tests
# ---------------------------------------------------------------------------

resource "aws_iam_user" "ci_user" {
  name = "gopherstack-iam-comprehensive-ci-user"
  path = "/comprehensive/"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Purpose     = "ci-testing"
  }
}

# ---------------------------------------------------------------------------
# Virtual MFA device
# ---------------------------------------------------------------------------

resource "aws_iam_virtual_mfa_device" "ci_user_mfa" {
  virtual_mfa_device_name = "gopherstack-iam-comprehensive-mfa"

  tags = {
    ManagedBy = "terraform"
    User      = aws_iam_user.ci_user.name
  }
}

# ---------------------------------------------------------------------------
# SSH public key for the CI user
# ---------------------------------------------------------------------------

resource "aws_iam_user_ssh_key" "ci_user_ssh" {
  username   = aws_iam_user.ci_user.name
  encoding   = "SSH"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC3eXDkzxW4qFSGDR7u6k1VJD5VDLVbQfr9P1/gopherstack-test"
}

# ---------------------------------------------------------------------------
# Service-specific credential (CodeCommit)
# ---------------------------------------------------------------------------

resource "aws_iam_service_specific_credential" "ci_user_codecommit" {
  service_name = "codecommit.amazonaws.com"
  user_name    = aws_iam_user.ci_user.name
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "oidc_provider_arn" {
  description = "ARN of the GitHub Actions OIDC provider"
  value       = aws_iam_openid_connect_provider.github.arn
}

output "oidc_role_arn" {
  description = "ARN of the OIDC-trusted IAM role"
  value       = aws_iam_role.oidc_role.arn
}

output "policy_arn" {
  description = "ARN of the read-only managed policy"
  value       = aws_iam_policy.read_only.arn
}

output "ci_user_arn" {
  description = "ARN of the CI IAM user"
  value       = aws_iam_user.ci_user.arn
}

output "virtual_mfa_serial" {
  description = "Serial number of the virtual MFA device"
  value       = aws_iam_virtual_mfa_device.ci_user_mfa.id
  sensitive   = true
}

output "ssh_key_id" {
  description = "ID of the uploaded SSH public key"
  value       = aws_iam_user_ssh_key.ci_user_ssh.ssh_public_key_id
}

output "codecommit_credential_id" {
  description = "ID of the CodeCommit service-specific credential"
  value       = aws_iam_service_specific_credential.ci_user_codecommit.service_specific_credential_id
}
