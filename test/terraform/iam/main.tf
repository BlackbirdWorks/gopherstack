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
# Permission boundary policy
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "boundary" {
  name        = "gopherstack-test-boundary-policy"
  description = "Permission boundary policy limiting ec2 and s3 actions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ec2:*", "s3:ListBucket"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "boundary_role" {
  name                 = "gopherstack-test-boundary-role"
  permissions_boundary = aws_iam_policy.boundary.arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

resource "aws_iam_role_policy" "boundary_role_inline" {
  name = "boundary-role-s3-inline"
  role = aws_iam_role.boundary_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject"]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------------------
# Access key lifecycle (create user + access key)
# ---------------------------------------------------------------------------

resource "aws_iam_user" "api_user" {
  name = "gopherstack-test-api-user"

  tags = {
    Purpose = "api-access"
  }
}

resource "aws_iam_access_key" "api_user_key" {
  user = aws_iam_user.api_user.name
}

resource "aws_iam_user_policy" "api_user_policy" {
  name = "api-user-s3-policy"
  user = aws_iam_user.api_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:ListAllMyBuckets", "s3:GetBucketLocation"]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM group with membership
# ---------------------------------------------------------------------------

resource "aws_iam_group" "developers" {
  name = "gopherstack-test-developers"
  path = "/engineering/"
}

resource "aws_iam_group_policy" "developers_inline" {
  name  = "developers-s3-read"
  group = aws_iam_group.developers.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_group_policy_attachment" "developers_managed" {
  group      = aws_iam_group.developers.name
  policy_arn = aws_iam_policy.managed.arn
}

resource "aws_iam_user" "dev_member" {
  name = "gopherstack-test-dev-member"

  tags = {
    Team = "engineering"
  }
}

resource "aws_iam_user_group_membership" "dev_member_groups" {
  user = aws_iam_user.dev_member.name
  groups = [
    aws_iam_group.developers.name,
  ]
}

# ---------------------------------------------------------------------------
# Additional service-linked role (Elastic Load Balancing)
# ---------------------------------------------------------------------------

resource "aws_iam_service_linked_role" "elb" {
  aws_service_name = "elasticloadbalancing.amazonaws.com"
  description      = "Service-linked role for ELB (gopherstack test)"
}

# ---------------------------------------------------------------------------
# Role with multiple policy attachments
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "read_only" {
  name        = "gopherstack-test-read-only"
  description = "Read-only policy for gopherstack test"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket", "ec2:Describe*", "iam:Get*", "iam:List*"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "multi_policy_role" {
  name = "gopherstack-test-multi-policy-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "multi_managed" {
  role       = aws_iam_role.multi_policy_role.name
  policy_arn = aws_iam_policy.managed.arn
}

resource "aws_iam_role_policy_attachment" "multi_read_only" {
  role       = aws_iam_role.multi_policy_role.name
  policy_arn = aws_iam_policy.read_only.arn
}

# ---------------------------------------------------------------------------
# Instance profile linked to app role
# ---------------------------------------------------------------------------

resource "aws_iam_instance_profile" "app_profile" {
  name = "gopherstack-test-app-profile"
  role = aws_iam_role.app.name
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

output "elb_service_linked_role_arn" {
  value       = aws_iam_service_linked_role.elb.arn
  description = "ARN of the ELB service-linked role"
}

output "bounded_user_arn" {
  value       = aws_iam_user.bounded.arn
  description = "ARN of the permission-bounded IAM user"
}

output "boundary_role_arn" {
  value       = aws_iam_role.boundary_role.arn
  description = "ARN of the permission-boundary-scoped IAM role"
}

output "api_user_access_key_id" {
  value       = aws_iam_access_key.api_user_key.id
  description = "Access key ID for the API user"
}

output "developers_group_arn" {
  value       = aws_iam_group.developers.arn
  description = "ARN of the developers IAM group"
}

output "dev_member_arn" {
  value       = aws_iam_user.dev_member.arn
  description = "ARN of the developer member user"
}

output "multi_policy_role_arn" {
  value       = aws_iam_role.multi_policy_role.arn
  description = "ARN of the role with multiple policy attachments"
}

output "app_instance_profile_arn" {
  value       = aws_iam_instance_profile.app_profile.arn
  description = "ARN of the application instance profile"
}
