resource "aws_cloudwatch_log_group" "app" {
  name              = "/{{.Prefix}}/app"
  retention_in_days = 7

  tags = {
    Environment = "test"
  }
}

resource "aws_cloudwatch_log_group" "audit" {
  name              = "/{{.Prefix}}/audit"
  retention_in_days = 30

  tags = {
    Environment = "test"
  }
}

resource "aws_cloudwatch_log_stream" "app_stream" {
  name           = "app"
  log_group_name = aws_cloudwatch_log_group.app.name
}

resource "aws_cloudwatch_log_metric_filter" "error_count" {
  name           = "{{.Prefix}}-errors"
  pattern        = "[level = ERROR, ...]"
  log_group_name = aws_cloudwatch_log_group.app.name

  metric_transformation {
    name          = "ErrorCount"
    namespace     = "{{.Prefix}}/Metrics"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_log_metric_filter" "latency" {
  name           = "{{.Prefix}}-latency"
  pattern        = "[timestamp, level, latency]"
  log_group_name = aws_cloudwatch_log_group.app.name

  metric_transformation {
    name          = "Latency"
    namespace     = "{{.Prefix}}/Metrics"
    value         = "$latency"
    default_value = "0"
    unit          = "Milliseconds"
  }
}

resource "aws_iam_role" "lambda_exec" {
  name = "{{.Prefix}}-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "log_processor" {
  function_name = "{{.Prefix}}-log-processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "public.ecr.aws/lambda/python:3.12"
}

resource "aws_cloudwatch_log_subscription_filter" "lambda_delivery" {
  name            = "{{.Prefix}}-lambda-filter"
  log_group_name  = aws_cloudwatch_log_group.app.name
  filter_pattern  = ""
  destination_arn = aws_lambda_function.log_processor.arn

  depends_on = [aws_lambda_function.log_processor]
}

resource "aws_sns_topic" "alarm_topic" {
  name = "{{.Prefix}}-alarms"
}

resource "aws_cloudwatch_log_subscription_filter" "audit_sns" {
  name            = "{{.Prefix}}-audit-filter"
  log_group_name  = aws_cloudwatch_log_group.audit.name
  filter_pattern  = "[timestamp, user, action, resource]"
  destination_arn = aws_sns_topic.alarm_topic.arn
  distribution    = "Random"
}
