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
    cognitoidentityprovider = var.gopherstack_endpoint
    cognitoidentity         = var.gopherstack_endpoint
    iam                     = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# User Pool with MFA and schema customisation
# ---------------------------------------------------------------------------

resource "aws_cognito_user_pool" "main" {
  name                = "gopherstack-cognito-pool"
  mfa_configuration   = "OFF"
  username_attributes = ["email"]

  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_numbers   = true
    require_symbols   = false
    require_uppercase = true
  }

  schema {
    name                = "department"
    attribute_data_type = "String"
    mutable             = true
    required            = false

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# User Pool Client
# ---------------------------------------------------------------------------

resource "aws_cognito_user_pool_client" "app" {
  name                                 = "gopherstack-app-client"
  user_pool_id                         = aws_cognito_user_pool.main.id
  generate_secret                      = false
  explicit_auth_flows                  = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
  prevent_user_existence_errors        = "ENABLED"
  enable_token_revocation              = true
}

# ---------------------------------------------------------------------------
# User Pool Domain  (required for hosted-UI and OIDC federation)
# ---------------------------------------------------------------------------

resource "aws_cognito_user_pool_domain" "main" {
  domain       = "gopherstack-test-domain"
  user_pool_id = aws_cognito_user_pool.main.id
}

# ---------------------------------------------------------------------------
# Identity Provider (OIDC example — stubbed attributes)
# ---------------------------------------------------------------------------

resource "aws_cognito_identity_provider" "oidc" {
  user_pool_id  = aws_cognito_user_pool.main.id
  provider_name = "ExampleOIDC"
  provider_type = "OIDC"

  provider_details = {
    client_id                     = "oidc-client-id"
    client_secret                 = "oidc-client-secret"
    attributes_request_method     = "GET"
    oidc_issuer                   = "https://example.com"
    authorize_scopes              = "openid email profile"
  }

  attribute_mapping = {
    email    = "email"
    username = "sub"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "user_pool_id" {
  value       = aws_cognito_user_pool.main.id
  description = "ID of the created Cognito User Pool"
}

output "user_pool_arn" {
  value       = aws_cognito_user_pool.main.arn
  description = "ARN of the created Cognito User Pool"
}

output "app_client_id" {
  value       = aws_cognito_user_pool_client.app.id
  description = "ID of the created User Pool Client"
}

output "domain" {
  value       = aws_cognito_user_pool_domain.main.domain
  description = "Cognito User Pool Domain"
}
