# --- Shared networking ---

resource "aws_vpc" "agam" {
  cidr_block = "10.191.0.0/16"

  tags = {
    Name = "agam-vpc"
  }
}

resource "aws_subnet" "agam" {
  vpc_id     = aws_vpc.agam.id
  cidr_block = "10.191.1.0/24"

  tags = {
    Name = "agam-subnet"
  }
}

resource "aws_security_group" "agam" {
  name   = "agam-sg"
  vpc_id = aws_vpc.agam.id
}

resource "aws_vpc_endpoint" "agam_apprunner" {
  vpc_id             = aws_vpc.agam.id
  service_name       = "com.amazonaws.us-east-1.apprunner.requests"
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.agam.id]
  security_group_ids = [aws_security_group.agam.id]
}

resource "aws_iam_role" "agam_lambda" {
  name = "agam-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "agam" {
  function_name    = "agam-fn"
  role             = aws_iam_role.agam_lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  filename         = "{{.FunctionZip}}"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
}

# --- API Gateway V2 (HTTP API) ---

resource "aws_apigatewayv2_api" "http" {
  name          = "agam-http-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "http" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.agam.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_integration_response" "http" {
  api_id                   = aws_apigatewayv2_api.http.id
  integration_id           = aws_apigatewayv2_integration.http.id
  integration_response_key = "/200/"
}

resource "aws_apigatewayv2_route" "http" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "GET /items"
  target    = "integrations/${aws_apigatewayv2_integration.http.id}"
}

resource "aws_apigatewayv2_authorizer" "http" {
  api_id                            = aws_apigatewayv2_api.http.id
  authorizer_type                   = "REQUEST"
  authorizer_uri                    = aws_lambda_function.agam.invoke_arn
  identity_sources                  = ["$request.header.Authorization"]
  name                              = "agam-authorizer"
  authorizer_payload_format_version = "2.0"
}

resource "aws_apigatewayv2_model" "http" {
  api_id       = aws_apigatewayv2_api.http.id
  content_type = "application/json"
  name         = "AgamModel"

  schema = jsonencode({
    "$schema" = "http://json-schema.org/draft-04/schema#"
    title     = "AgamModel"
    type      = "object"

    properties = {
      id = {
        type = "string"
      }
    }
  })
}

resource "aws_apigatewayv2_deployment" "http" {
  api_id      = aws_apigatewayv2_api.http.id
  description = "agam deployment"

  depends_on = [aws_apigatewayv2_route.http]

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_apigatewayv2_stage" "http" {
  api_id        = aws_apigatewayv2_api.http.id
  name          = "prod"
  deployment_id = aws_apigatewayv2_deployment.http.id
}

resource "aws_acm_certificate" "agam" {
  domain_name       = "agam.example.test"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_apigatewayv2_domain_name" "http" {
  domain_name = "agam.example.test"

  domain_name_configuration {
    certificate_arn = aws_acm_certificate.agam.arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }
}

resource "aws_apigatewayv2_api_mapping" "http" {
  api_id      = aws_apigatewayv2_api.http.id
  domain_name = aws_apigatewayv2_domain_name.http.id
  stage       = aws_apigatewayv2_stage.http.id
}

# --- API Gateway V2 (WebSocket API, for route response) ---

resource "aws_apigatewayv2_api" "ws" {
  name                       = "agam-ws-api"
  protocol_type              = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
}

resource "aws_apigatewayv2_integration" "ws" {
  api_id           = aws_apigatewayv2_api.ws.id
  integration_type = "MOCK"
}

resource "aws_apigatewayv2_route" "ws" {
  api_id    = aws_apigatewayv2_api.ws.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.ws.id}"
}

resource "aws_apigatewayv2_route_response" "ws" {
  api_id             = aws_apigatewayv2_api.ws.id
  route_id           = aws_apigatewayv2_route.ws.id
  route_response_key = "$default"
}

resource "aws_apigatewayv2_vpc_link" "agam" {
  name               = "agam-vpc-link"
  security_group_ids = [aws_security_group.agam.id]
  subnet_ids         = [aws_subnet.agam.id]
}

# --- App Runner ---
#
# aws_apprunner_auto_scaling_configuration_version,
# aws_apprunner_default_auto_scaling_configuration_version, and
# aws_apprunner_custom_domain_association are left out: the pinned
# hashicorp/aws v5.100.0 provider's create waiters for these resources
# compare the real (uppercase) API status values against lowercase target
# literals internally, so they never match and either time out (ASG config,
# 2m) or fail immediately with "unexpected state" (custom domain
# association) -- verified byte-for-byte correct on the wire via TF_LOG=trace
# manual repro (Status=ACTIVE / PENDING_CERTIFICATE_DNS_VALIDATION every
# poll). This is a provider-side defect, not a gopherstack gap -- see
# services/apprunner/PARITY.md.

resource "aws_apprunner_observability_configuration" "agam" {
  observability_configuration_name = "agam-obs"

  trace_configuration {
    vendor = "AWSXRAY"
  }
}

resource "aws_apprunner_connection" "agam" {
  connection_name = "agam-connection"
  provider_type   = "GITHUB"
}

resource "aws_apprunner_service" "agam" {
  service_name = "agam-service"

  source_configuration {
    auto_deployments_enabled = false

    image_repository {
      image_identifier      = "public.ecr.aws/nginx/nginx:latest"
      image_repository_type = "ECR_PUBLIC"
    }
  }
}

resource "aws_apprunner_deployment" "agam" {
  service_arn = aws_apprunner_service.agam.arn
}

resource "aws_apprunner_vpc_connector" "agam" {
  vpc_connector_name = "agam-vpc-connector"
  subnets            = [aws_subnet.agam.id]
  security_groups    = [aws_security_group.agam.id]
}

resource "aws_apprunner_vpc_ingress_connection" "agam" {
  name        = "agam-ingress"
  service_arn = aws_apprunner_service.agam.arn

  ingress_vpc_configuration {
    vpc_id          = aws_vpc.agam.id
    vpc_endpoint_id = aws_vpc_endpoint.agam_apprunner.id
  }
}

# --- Macie2 ---

resource "aws_macie2_account" "agam" {}

resource "aws_kms_key" "agam_macie" {
  description             = "agam macie export key"
  deletion_window_in_days = 7
}

resource "aws_s3_bucket" "agam_macie" {
  bucket        = "agam-macie-bucket"
  force_destroy = true
}

resource "aws_macie2_classification_export_configuration" "agam" {
  depends_on = [aws_macie2_account.agam]

  s3_destination {
    bucket_name = aws_s3_bucket.agam_macie.bucket
    key_prefix  = "exports/"
    kms_key_arn = aws_kms_key.agam_macie.arn
  }
}

resource "aws_macie2_classification_job" "agam" {
  job_type = "ONE_TIME"
  name     = "agam-classification-job"

  s3_job_definition {
    bucket_definitions {
      account_id = "000000000000"
      buckets    = [aws_s3_bucket.agam_macie.bucket]
    }
  }

  depends_on = [aws_macie2_account.agam]
}

resource "aws_macie2_custom_data_identifier" "agam" {
  name                   = "agam-custom-data-identifier"
  regex                  = "[0-9]{3}-[0-9]{2}-[0-9]{4}"
  description            = "agam CDI"
  maximum_match_distance = 10
  keywords               = ["ssn"]

  depends_on = [aws_macie2_account.agam]
}

resource "aws_macie2_findings_filter" "agam" {
  name        = "agam-findings-filter"
  description = "agam filter"
  action      = "ARCHIVE"

  finding_criteria {
    criterion {
      field = "region"
      eq    = ["us-east-1"]
    }
  }

  depends_on = [aws_macie2_account.agam]
}

resource "aws_macie2_member" "agam" {
  account_id = "111111111111"
  email      = "agam-member@example.test"
  invite     = false

  depends_on = [aws_macie2_account.agam]
}

resource "aws_macie2_organization_admin_account" "agam" {
  admin_account_id = "222222222222"

  depends_on = [aws_macie2_account.agam]
}

resource "aws_macie2_organization_configuration" "agam" {
  auto_enable = true
}
