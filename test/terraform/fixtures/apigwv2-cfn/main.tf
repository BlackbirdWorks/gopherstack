resource "aws_iam_role" "lambda" {
  name = "{{.RoleName}}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "handler" {
  filename         = "{{.ZipPath}}"
  function_name    = "{{.FuncName}}"
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.ZipPath}}")
}

resource "aws_cloudformation_stack" "apigwv2" {
  name = "{{.StackName}}"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "APIGW v2 full stack via CloudFormation"
    Resources = {
      HttpApi = {
        Type = "AWS::ApiGatewayV2::Api"
        Properties = {
          Name          = "{{.APIName}}"
          ProtocolType  = "HTTP"
          Description   = "Test HTTP API via CFN"
        }
      }
      Integration = {
        Type = "AWS::ApiGatewayV2::Integration"
        Properties = {
          ApiId           = { Ref = "HttpApi" }
          IntegrationType = "AWS_PROXY"
          IntegrationUri  = aws_lambda_function.handler.arn
          PayloadFormatVersion = "2.0"
        }
      }
      Route = {
        Type = "AWS::ApiGatewayV2::Route"
        Properties = {
          ApiId    = { Ref = "HttpApi" }
          RouteKey = "GET /hello"
          Target   = { "Fn::Join" = ["", ["integrations/", { Ref = "Integration" }]] }
        }
      }
      Stage = {
        Type = "AWS::ApiGatewayV2::Stage"
        Properties = {
          ApiId      = { Ref = "HttpApi" }
          StageName  = "$default"
          AutoDeploy = true
        }
      }
    }
    Outputs = {
      ApiId = {
        Value = { Ref = "HttpApi" }
      }
    }
  })

  depends_on = [aws_lambda_function.handler]
}
