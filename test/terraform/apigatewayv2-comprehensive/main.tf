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
    apigatewayv2 = var.gopherstack_endpoint
    acm          = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "GopherStack local endpoint URL"
  default     = "http://localhost:4566"
}

# --------------------------------------------------------------------------
# HTTP API with JWT Authorizer
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_api" "http" {
  name          = "comprehensive-http-api"
  protocol_type = "HTTP"
  description   = "Comprehensive HTTP API for testing"
  version       = "1.0"

  cors_configuration {
    allow_origins = ["https://example.com"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["Content-Type", "Authorization"]
    max_age       = 300
  }

  tags = {
    Environment = "test"
    Project     = "gopherstack"
  }
}

resource "aws_apigatewayv2_authorizer" "jwt" {
  api_id           = aws_apigatewayv2_api.http.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  name             = "cognito-jwt-authorizer"

  jwt_configuration {
    audience = ["https://api.example.com", "client-app-id"]
    issuer   = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_TestPool"
  }
}

# --------------------------------------------------------------------------
# Routes with and without JWT authorization
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_route" "public" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "GET /health"
}

resource "aws_apigatewayv2_route" "protected_list" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "GET /items"
  authorization_type = "JWT"
  authorizer_id      = aws_apigatewayv2_authorizer.jwt.id
  operation_name     = "ListItems"
}

resource "aws_apigatewayv2_route" "protected_create" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "POST /items"
  authorization_type = "JWT"
  authorizer_id      = aws_apigatewayv2_authorizer.jwt.id
  operation_name     = "CreateItem"
}

# --------------------------------------------------------------------------
# Integration (backend target)
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = "arn:aws:lambda:us-east-1:123456789012:function:my-handler"
  payload_format_version = "2.0"
  description            = "Lambda proxy integration"
}

# --------------------------------------------------------------------------
# VPC Link for private integrations
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_vpc_link" "private" {
  name               = "private-vpc-link"
  subnet_ids         = ["subnet-aaa111", "subnet-bbb222"]
  security_group_ids = ["sg-111aaa", "sg-222bbb"]

  tags = {
    Environment = "test"
  }
}

# --------------------------------------------------------------------------
# Stages with access log settings and per-route throttling
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_stage" "prod" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "prod"
  auto_deploy = true
  description = "Production stage"

  access_log_settings {
    destination_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/comprehensive-http-api/prod"
  }

  default_route_settings {
    throttling_rate_limit  = 1000
    throttling_burst_limit = 500
    logging_level          = "INFO"
  }

  route_settings {
    route_key              = "GET /items"
    throttling_rate_limit  = 100
    throttling_burst_limit = 50
  }

  route_settings {
    route_key              = "POST /items"
    throttling_rate_limit  = 50
    throttling_burst_limit = 25
  }

  stage_variables = {
    "log_level" = "INFO"
    "env"       = "production"
  }

  tags = {
    Environment = "prod"
  }
}

resource "aws_apigatewayv2_stage" "staging" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "staging"
  auto_deploy = true
  description = "Staging stage"

  access_log_settings {
    destination_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/comprehensive-http-api/staging"
  }
}

# --------------------------------------------------------------------------
# Model with JSON schema
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_model" "item" {
  api_id       = aws_apigatewayv2_api.http.id
  content_type = "application/json"
  name         = "Item"
  description  = "An item model"

  schema = jsonencode({
    "$schema" = "http://json-schema.org/draft-04/schema#"
    title     = "Item"
    type      = "object"
    properties = {
      id = {
        type = "string"
      }
      name = {
        type = "string"
      }
      price = {
        type   = "number"
        format = "double"
      }
    }
    required = ["id", "name"]
  })
}

# --------------------------------------------------------------------------
# Domain name with certificate
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_domain_name" "api" {
  domain_name = "api.example.com"

  domain_name_configuration {
    certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/abc123def-456-789-abc-123def456789"
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }

  tags = {
    Environment = "test"
  }
}

# --------------------------------------------------------------------------
# API Mappings linking domain name to API stages
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_api_mapping" "prod" {
  api_id          = aws_apigatewayv2_api.http.id
  domain_name     = aws_apigatewayv2_domain_name.api.id
  stage           = aws_apigatewayv2_stage.prod.id
  api_mapping_key = "v1"
}

resource "aws_apigatewayv2_api_mapping" "staging" {
  api_id          = aws_apigatewayv2_api.http.id
  domain_name     = aws_apigatewayv2_domain_name.api.id
  stage           = aws_apigatewayv2_stage.staging.id
  api_mapping_key = "v1-staging"
}

# --------------------------------------------------------------------------
# WebSocket API
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_api" "websocket" {
  name                       = "comprehensive-ws-api"
  protocol_type              = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
  description                = "WebSocket API for real-time features"

  tags = {
    Environment = "test"
    Type        = "websocket"
  }
}

resource "aws_apigatewayv2_route" "ws_connect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$connect"
}

resource "aws_apigatewayv2_route" "ws_disconnect" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$disconnect"
}

resource "aws_apigatewayv2_route" "ws_default" {
  api_id    = aws_apigatewayv2_api.websocket.id
  route_key = "$default"
}

resource "aws_apigatewayv2_stage" "ws_prod" {
  api_id      = aws_apigatewayv2_api.websocket.id
  name        = "prod"
  auto_deploy = true

  access_log_settings {
    destination_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/comprehensive-ws-api/prod"
  }
}

# --------------------------------------------------------------------------
# Deployment
# --------------------------------------------------------------------------

resource "aws_apigatewayv2_deployment" "prod" {
  api_id      = aws_apigatewayv2_api.http.id
  description = "Production deployment"

  depends_on = [
    aws_apigatewayv2_route.public,
    aws_apigatewayv2_route.protected_list,
    aws_apigatewayv2_route.protected_create,
    aws_apigatewayv2_integration.lambda,
  ]
}

# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------

output "http_api_id" {
  value = aws_apigatewayv2_api.http.id
}

output "http_api_endpoint" {
  value = aws_apigatewayv2_api.http.api_endpoint
}

output "jwt_authorizer_id" {
  value = aws_apigatewayv2_authorizer.jwt.id
}

output "vpc_link_id" {
  value = aws_apigatewayv2_vpc_link.private.id
}

output "vpc_link_status" {
  value = aws_apigatewayv2_vpc_link.private.vpc_link_status
}

output "domain_name_id" {
  value = aws_apigatewayv2_domain_name.api.id
}

output "prod_api_mapping_id" {
  value = aws_apigatewayv2_api_mapping.prod.id
}

output "item_model_id" {
  value = aws_apigatewayv2_model.item.id
}

output "websocket_api_id" {
  value = aws_apigatewayv2_api.websocket.id
}

output "prod_stage_id" {
  value = aws_apigatewayv2_stage.prod.id
}
