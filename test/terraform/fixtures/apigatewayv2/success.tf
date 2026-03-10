resource "aws_apigatewayv2_api" "this" {
  name          = "{{.APIName}}"
  protocol_type = "HTTP"
  description   = "Test HTTP API"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.this.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "this" {
  api_id           = aws_apigatewayv2_api.this.id
  integration_type = "MOCK"
}

resource "aws_apigatewayv2_route" "this" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "GET /items"
  target    = "integrations/${aws_apigatewayv2_integration.this.id}"
}
