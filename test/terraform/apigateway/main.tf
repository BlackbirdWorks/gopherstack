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
    apigateway = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

resource "aws_api_gateway_rest_api" "example" {
  name        = "gopherstack-test-api"
  description = "Test REST API for gopherstack"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Resource and Method
# ---------------------------------------------------------------------------

resource "aws_api_gateway_resource" "items" {
  rest_api_id = aws_api_gateway_rest_api.example.id
  parent_id   = aws_api_gateway_rest_api.example.root_resource_id
  path_part   = "items"
}

resource "aws_api_gateway_method" "get_items" {
  rest_api_id   = aws_api_gateway_rest_api.example.id
  resource_id   = aws_api_gateway_resource.items.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "get_items" {
  rest_api_id = aws_api_gateway_rest_api.example.id
  resource_id = aws_api_gateway_resource.items.id
  http_method = aws_api_gateway_method.get_items.http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

# ---------------------------------------------------------------------------
# Deployment and Stage
# ---------------------------------------------------------------------------

resource "aws_api_gateway_deployment" "example" {
  rest_api_id = aws_api_gateway_rest_api.example.id

  depends_on = [
    aws_api_gateway_integration.get_items,
  ]
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.example.id
  deployment_id = aws_api_gateway_deployment.example.id
  stage_name    = "v1"

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# API Key
# ---------------------------------------------------------------------------

resource "aws_api_gateway_api_key" "example" {
  name    = "gopherstack-test-key"
  enabled = true

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# Usage Plan with throttle and quota
# ---------------------------------------------------------------------------

resource "aws_api_gateway_usage_plan" "example" {
  name        = "gopherstack-test-plan"
  description = "Test usage plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.example.id
    stage  = aws_api_gateway_stage.v1.stage_name
  }

  throttle_settings {
    burst_limit = 100
    rate_limit  = 50
  }

  quota_settings {
    limit  = 1000
    period = "MONTH"
  }

  tags = {
    Environment = "test"
  }
}

resource "aws_api_gateway_usage_plan_key" "example" {
  key_id        = aws_api_gateway_api_key.example.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.example.id
}

# ---------------------------------------------------------------------------
# VPC Link
# ---------------------------------------------------------------------------

resource "aws_api_gateway_vpc_link" "example" {
  name        = "gopherstack-test-vpc-link"
  description = "Test VPC link"
  target_arns = ["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/test/1234567890abcdef"]

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "api_id" {
  value = aws_api_gateway_rest_api.example.id
}

output "stage_invoke_url" {
  value = aws_api_gateway_stage.v1.invoke_url
}

output "api_key_value" {
  value     = aws_api_gateway_api_key.example.value
  sensitive = true
}

output "usage_plan_id" {
  value = aws_api_gateway_usage_plan.example.id
}

output "vpc_link_id" {
  value = aws_api_gateway_vpc_link.example.id
}
