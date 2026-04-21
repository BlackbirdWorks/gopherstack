resource "aws_cloudwatch_event_bus" "this" {
  name = "{{.BusName}}"
}

resource "aws_cloudwatch_event_rule" "this" {
  name                = "{{.RuleName}}"
  event_bus_name      = aws_cloudwatch_event_bus.this.name
  schedule_expression = "rate(5 minutes)"
  description         = "Managed by gopherstack terraform test"
}

resource "aws_cloudwatch_event_archive" "this" {
  name             = "{{.ArchiveName}}"
  event_source_arn = aws_cloudwatch_event_bus.this.arn
  retention_days   = 7
  description      = "Test archive managed by gopherstack"
}

resource "aws_cloudwatch_event_connection" "this" {
  name               = "{{.ConnectionName}}"
  authorization_type = "API_KEY"

  auth_parameters {
    api_key {
      key   = "x-api-key"
      value = "test-secret-value"
    }
  }
}

resource "aws_cloudwatch_event_api_destination" "this" {
  name                             = "{{.APIDestinationName}}"
  connection_arn                   = aws_cloudwatch_event_connection.this.arn
  http_method                      = "POST"
  invocation_endpoint              = "https://httpbin.org/post"
  invocation_rate_limit_per_second = 20
  description                      = "Test API destination"
}
