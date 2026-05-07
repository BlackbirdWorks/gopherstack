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
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Log group with retention policy
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "app" {
  name              = "/gopherstack/app"
  retention_in_days = 7

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Log stream inside the group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_stream" "app" {
  name           = "app-stream"
  log_group_name = aws_cloudwatch_log_group.app.name
}

# ---------------------------------------------------------------------------
# Metric filter — emit a CloudWatch metric for every ERROR log line
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "error_count" {
  name           = "error-count"
  log_group_name = aws_cloudwatch_log_group.app.name
  pattern        = "ERROR"

  metric_transformation {
    name      = "ErrorCount"
    namespace = "GopherstackApp"
    value     = "1"
  }

  depends_on = [aws_cloudwatch_log_group.app]
}

# ---------------------------------------------------------------------------
# Subscription filter — deliver all log events to a second log group
# (cross-service delivery; destination ARN uses a dummy Lambda ARN for mock)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "audit" {
  name              = "/gopherstack/audit"
  retention_in_days = 14
}

# ---------------------------------------------------------------------------
# CloudWatch alarm on the metric emitted by the filter above
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name          = "high-error-rate"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "GopherstackApp"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Fires when ErrorCount >= 5 in a 60s window"

  depends_on = [aws_cloudwatch_log_metric_filter.error_count]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "log_group_arn" {
  value = aws_cloudwatch_log_group.app.arn
}

output "log_stream_name" {
  value = aws_cloudwatch_log_stream.app.name
}

output "metric_filter_name" {
  value = aws_cloudwatch_log_metric_filter.error_count.name
}

output "alarm_name" {
  value = aws_cloudwatch_metric_alarm.error_alarm.alarm_name
}
