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
  skip_requesting_account_id = true

  endpoints {
    iam = var.gopherstack_endpoint
    sts = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack IAM endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Role with inline policy
# ---------------------------------------------------------------------------

resource "aws_iam_role" "app" {
  name = "gopherstack-test-app-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# Inline policy attached directly to the role (aws_iam_role_policy)
resource "aws_iam_role_policy" "app_inline" {
  name = "app-inline-s3-policy"
  role = aws_iam_role.app.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------------------
# Managed policy with version management
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "managed" {
  name        = "gopherstack-test-managed-policy"
  description = "Managed policy for gopherstack IAM test"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ec2:DescribeInstances", "ec2:DescribeSecurityGroups"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "app_managed" {
  role       = aws_iam_role.app.name
  policy_arn = aws_iam_policy.managed.arn
}

# ---------------------------------------------------------------------------
# Service-linked role (EKS)
# ---------------------------------------------------------------------------

resource "aws_iam_service_linked_role" "eks" {
  aws_service_name = "eks.amazonaws.com"
  description      = "Service-linked role for EKS (gopherstack test)"
}

# ---------------------------------------------------------------------------
# Permission boundary on a user
# ---------------------------------------------------------------------------

resource "aws_iam_user" "bounded" {
  name = "gopherstack-test-bounded-user"

  tags = {
    Environment = "test"
  }
}

resource "aws_iam_user_policy" "bounded_inline" {
  name = "bounded-user-inline-policy"
  user = aws_iam_user.bounded.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sts:GetCallerIdentity"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_user_policy_attachment" "bounded_managed" {
  user       = aws_iam_user.bounded.name
  policy_arn = aws_iam_policy.managed.arn
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "app_role_arn" {
  value       = aws_iam_role.app.arn
  description = "ARN of the application IAM role"
}

output "managed_policy_arn" {
  value       = aws_iam_policy.managed.arn
  description = "ARN of the managed IAM policy"
}

output "eks_service_linked_role_arn" {
  value       = aws_iam_service_linked_role.eks.arn
  description = "ARN of the EKS service-linked role"
}

output "bounded_user_arn" {
  value       = aws_iam_user.bounded.arn
  description = "ARN of the permission-bounded IAM user"
}
