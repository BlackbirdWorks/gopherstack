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
    cloudwatch     = var.gopherstack_endpoint
    cloudwatchlogs = var.gopherstack_endpoint
    iam            = var.gopherstack_endpoint
    lambda         = var.gopherstack_endpoint
    sns            = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SNS topic for alarm actions
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alarm_target" {
  name = "cw-comprehensive-alarm-topic"
}

# ---------------------------------------------------------------------------
# Metric alarm with history (alarm state transitions are tracked on creation)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "cpu_high" {
  alarm_name          = "cw-comp-cpu-high"
  alarm_description   = "CPU > 80% for 2 consecutive periods"
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 80
  evaluation_periods  = 2
  period              = 60
  statistic           = "Average"
  treat_missing_data  = "notBreaching"
  actions_enabled     = true
  alarm_actions       = [aws_sns_topic.alarm_target.arn]
  ok_actions          = [aws_sns_topic.alarm_target.arn]

  tags = {
    Environment = "test"
    Feature     = "alarm-history"
  }
}

# ---------------------------------------------------------------------------
# Second metric alarm (referenced by composite alarm)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "error_rate" {
  alarm_name          = "cw-comp-error-rate"
  alarm_description   = "Error rate > 5%"
  namespace           = "MyApp/API"
  metric_name         = "ErrorRate"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 5
  evaluation_periods  = 1
  period              = 60
  statistic           = "Average"
  treat_missing_data  = "notBreaching"
}

# ---------------------------------------------------------------------------
# Composite alarm combining the two metric alarms
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_composite_alarm" "critical_service" {
  alarm_name        = "cw-comp-critical-service"
  alarm_description = "Critical when CPU high OR error rate elevated"
  alarm_rule        = "ALARM(${aws_cloudwatch_metric_alarm.cpu_high.alarm_name}) OR ALARM(${aws_cloudwatch_metric_alarm.error_rate.alarm_name})"
  alarm_actions     = [aws_sns_topic.alarm_target.arn]
  ok_actions        = [aws_sns_topic.alarm_target.arn]
  actions_enabled   = true

  depends_on = [
    aws_cloudwatch_metric_alarm.cpu_high,
    aws_cloudwatch_metric_alarm.error_rate,
  ]

  tags = {
    Feature = "composite-alarms"
  }
}

# ---------------------------------------------------------------------------
# Anomaly detector
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "anomaly_alarm" {
  alarm_name          = "cw-comp-anomaly-alarm"
  alarm_description   = "Alarm when metric deviates from expected band"
  namespace           = "MyApp/API"
  metric_name         = "Latency"
  comparison_operator = "GreaterThanUpperThreshold"
  threshold_metric_id = "ad1"
  evaluation_periods  = 2
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "m1"
    return_data = true

    metric {
      namespace   = "MyApp/API"
      metric_name = "Latency"
      period      = 60
      stat        = "Average"
    }
  }

  metric_query {
    id          = "ad1"
    expression  = "ANOMALY_DETECTION_BAND(m1, 2)"
    label       = "Latency (expected)"
    return_data = true
  }
}

# ---------------------------------------------------------------------------
# Log group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "app" {
  name              = "/cw-comprehensive/app"
  retention_in_days = 14

  tags = {
    Feature = "log-groups"
  }
}

# ---------------------------------------------------------------------------
# Log stream
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_stream" "app" {
  name           = "app-stream"
  log_group_name = aws_cloudwatch_log_group.app.name
}

# ---------------------------------------------------------------------------
# Metric filter on the log group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "error_count" {
  name           = "cw-comp-error-count"
  pattern        = "[level = ERROR, ...]"
  log_group_name = aws_cloudwatch_log_group.app.name

  metric_transformation {
    name          = "ErrorCount"
    namespace     = "MyApp/Logs"
    value         = "1"
    default_value = "0"
  }
}

# ---------------------------------------------------------------------------
# IAM role for Lambda (dummy — gopherstack doesn't enforce IAM)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_exec" {
  name = "cw-comp-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ---------------------------------------------------------------------------
# Lambda function used as subscription filter destination
# (container-image based — no zip file required)
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "log_processor" {
  function_name = "cw-comp-log-processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "public.ecr.aws/lambda/python:3.12"

  environment {
    variables = {
      LOG_LEVEL = "INFO"
    }
  }

  tags = {
    Feature = "subscription-filter-lambda"
  }
}

# ---------------------------------------------------------------------------
# Subscription filter: deliver log events to Lambda
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_subscription_filter" "lambda_delivery" {
  name            = "cw-comp-lambda-delivery"
  log_group_name  = aws_cloudwatch_log_group.app.name
  filter_pattern  = ""
  destination_arn = aws_lambda_function.log_processor.arn

  depends_on = [aws_lambda_function.log_processor]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "metric_alarm_arn" {
  value = aws_cloudwatch_metric_alarm.cpu_high.arn
}

output "composite_alarm_arn" {
  value = aws_cloudwatch_composite_alarm.critical_service.arn
}

output "log_group_arn" {
  value = aws_cloudwatch_log_group.app.arn
}

output "subscription_filter_log_group" {
  value = aws_cloudwatch_log_subscription_filter.lambda_delivery.log_group_name
}
