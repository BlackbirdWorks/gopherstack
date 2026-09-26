# --- Lambda -------------------------------------------------------------

resource "aws_iam_role" "lambda" {
  name = "lagw-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "this" {
  filename         = "{{.FunctionZip}}"
  function_name    = "lagw-function"
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
  publish          = true
}

resource "aws_lambda_alias" "this" {
  name             = "live"
  description      = "lagw alias"
  function_name    = aws_lambda_function.this.function_name
  function_version = aws_lambda_function.this.version
}

resource "aws_lambda_code_signing_config" "this" {
  allowed_publishers {
    signing_profile_version_arns = [
      "arn:aws:signer:us-east-1:000000000000:/signing-profiles/lagw/AbCdEfGhIj",
    ]
  }

  policies {
    untrusted_artifact_on_deployment = "Warn"
  }

  description = "lagw code signing config"
}

resource "aws_lambda_function_event_invoke_config" "this" {
  function_name          = aws_lambda_function.this.function_name
  maximum_retry_attempts = 1

  destination_config {
    on_failure {
      destination = aws_lambda_function.this.arn
    }
  }
}

resource "aws_lambda_function_recursion_config" "this" {
  function_name  = aws_lambda_function.this.function_name
  recursive_loop = "Allow"
}

resource "aws_lambda_function_url" "this" {
  function_name      = aws_lambda_function.this.function_name
  authorization_type = "NONE"
}

resource "aws_lambda_runtime_management_config" "this" {
  function_name     = aws_lambda_function.this.function_name
  update_runtime_on = "Auto"
}

resource "aws_lambda_layer_version" "this" {
  layer_name          = "lagw-layer"
  filename            = "{{.LayerZip}}"
  source_code_hash    = filebase64sha256("{{.LayerZip}}")
  compatible_runtimes = ["python3.12"]
}

resource "aws_lambda_layer_version_permission" "this" {
  layer_name     = aws_lambda_layer_version.this.layer_name
  version_number = aws_lambda_layer_version.this.version
  statement_id   = "lagw-layer-perm"
  action         = "lambda:GetLayerVersion"
  principal      = "*"
}

# --- API Gateway ----------------------------------------------------------

resource "aws_iam_role" "apigw_cloudwatch" {
  name = "lagw-apigw-cw-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "apigateway.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_api_gateway_account" "this" {
  cloudwatch_role_arn = aws_iam_role.apigw_cloudwatch.arn
}

resource "aws_api_gateway_rest_api" "this" {
  name = "lagw-api"
}

resource "aws_api_gateway_resource" "items" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "items"
}

resource "aws_api_gateway_method" "get" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = aws_api_gateway_resource.items.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "get" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.items.id
  http_method = aws_api_gateway_method.get.http_method
  type        = "MOCK"
}

resource "aws_api_gateway_method_response" "response_200" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.items.id
  http_method = aws_api_gateway_method.get.http_method
  status_code = "200"
}

resource "aws_api_gateway_integration_response" "response_200" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.items.id
  http_method = aws_api_gateway_method.get.http_method
  status_code = aws_api_gateway_method_response.response_200.status_code

  depends_on = [aws_api_gateway_integration.get]
}

resource "aws_api_gateway_deployment" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  depends_on = [
    aws_api_gateway_method.get,
    aws_api_gateway_integration.get,
    aws_api_gateway_method_response.response_200,
    aws_api_gateway_integration_response.response_200,
  ]
}

resource "aws_api_gateway_stage" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  deployment_id = aws_api_gateway_deployment.this.id
  stage_name    = "prod"
}

resource "aws_api_gateway_method_settings" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  stage_name  = aws_api_gateway_stage.this.stage_name
  method_path = "*/*"

  settings {
    metrics_enabled = true
    logging_level   = "INFO"
  }
}

resource "aws_api_gateway_gateway_response" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  response_type = "DEFAULT_4XX"
  status_code   = "404"

  response_templates = {
    "application/json" = "{\"message\":$context.error.messageString}"
  }
}

resource "aws_api_gateway_documentation_part" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  location {
    type = "API"
  }

  properties = jsonencode({ description = "lagw API docs" })
}

resource "aws_api_gateway_documentation_version" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  version     = "v1"

  depends_on = [aws_api_gateway_documentation_part.this]
}

resource "aws_api_gateway_model" "this" {
  rest_api_id  = aws_api_gateway_rest_api.this.id
  name         = "ItemModel"
  description  = "lagw item model"
  content_type = "application/json"

  schema = jsonencode({
    "$schema" = "http://json-schema.org/draft-04/schema#"
    title     = "ItemModel"
    type      = "object"
    properties = {
      id = { type = "string" }
    }
  })
}

resource "aws_api_gateway_rest_api_policy" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "execute-api:Invoke"
      Resource  = "execute-api:/*"
    }]
  })
}

resource "aws_api_gateway_api_key" "this" {
  name    = "lagw-key"
  enabled = true
}

resource "aws_api_gateway_usage_plan" "this" {
  name = "lagw-usage-plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.this.id
    stage  = aws_api_gateway_stage.this.stage_name
  }
}

resource "aws_api_gateway_usage_plan_key" "this" {
  usage_plan_id = aws_api_gateway_usage_plan.this.id
  key_id        = aws_api_gateway_api_key.this.id
  key_type      = "API_KEY"
}

resource "aws_api_gateway_authorizer" "this" {
  rest_api_id     = aws_api_gateway_rest_api.this.id
  name            = "lagw-authorizer"
  type            = "TOKEN"
  authorizer_uri  = aws_lambda_function.this.invoke_arn
  identity_source = "method.request.header.Authorization"
}

resource "aws_api_gateway_client_certificate" "this" {
  description = "lagw client certificate"
}

resource "aws_acm_certificate" "domain" {
  domain_name       = "lagw.example.test"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_domain_name" "this" {
  domain_name              = "lagw.example.test"
  regional_certificate_arn = aws_acm_certificate.domain.arn
  security_policy          = "TLS_1_2"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_base_path_mapping" "this" {
  api_id      = aws_api_gateway_rest_api.this.id
  stage_name  = aws_api_gateway_stage.this.stage_name
  domain_name = aws_api_gateway_domain_name.this.domain_name
  base_path   = "v1"
}

resource "aws_vpc" "vpclink" {
  cidr_block = "10.78.0.0/16"

  tags = {
    Name = "lagw-vpc"
  }
}

resource "aws_subnet" "vpclink" {
  vpc_id     = aws_vpc.vpclink.id
  cidr_block = "10.78.1.0/24"

  tags = {
    Name = "lagw-subnet"
  }
}

resource "aws_lb" "nlb" {
  name               = "lagw-nlb"
  internal           = true
  load_balancer_type = "network"
  subnets            = [aws_subnet.vpclink.id]
}

resource "aws_api_gateway_vpc_link" "this" {
  name        = "lagw-vpc-link"
  description = "lagw VPC link"
  target_arns = [aws_lb.nlb.arn]
}
