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
    cloudwatch = var.gopherstack_endpoint
    kinesis    = var.gopherstack_endpoint
    iam        = var.gopherstack_endpoint
    sns        = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SNS topic used as alarm action target
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alarm_target" {
  name = "cloudwatch-alarm-topic"
}

# ---------------------------------------------------------------------------
# Basic metric alarm
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  alarm_name          = "high-cpu-utilization"
  alarm_description   = "Triggers when CPU exceeds 80 percent"
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
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Extended-statistic alarm (p99)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "p99_latency" {
  alarm_name          = "p99-latency"
  alarm_description   = "Triggers when p99 latency exceeds 500 ms"
  namespace           = "MyApp/API"
  metric_name         = "RequestLatency"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 500
  evaluation_periods  = 1
  period              = 60
  extended_statistic  = "p99"
  treat_missing_data  = "notBreaching"
}

# ---------------------------------------------------------------------------
# Composite alarm
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_composite_alarm" "service_down" {
  alarm_name        = "service-down"
  alarm_description = "Composite: fires when CPU is high AND latency is high"
  alarm_rule        = "ALARM(${aws_cloudwatch_metric_alarm.high_cpu.alarm_name}) AND ALARM(${aws_cloudwatch_metric_alarm.p99_latency.alarm_name})"
  alarm_actions     = [aws_sns_topic.alarm_target.arn]
}

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "gopherstack-test-dashboard"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          metrics = [
            ["AWS/EC2", "CPUUtilization"]
          ]
          period = 60
          stat   = "Average"
          title  = "CPU Utilization"
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Metric stream
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_stream" "main" {
  name          = "gopherstack-metric-stream"
  role_arn      = "arn:aws:iam::000000000000:role/metric-stream-role"
  firehose_arn  = "arn:aws:firehose:us-east-1:000000000000:deliverystream/metric-stream"
  output_format = "json"

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "alarm_arn" {
  description = "ARN of the high-CPU metric alarm"
  value       = aws_cloudwatch_metric_alarm.high_cpu.arn
}

output "composite_alarm_arn" {
  description = "ARN of the composite alarm"
  value       = aws_cloudwatch_composite_alarm.service_down.arn
}

output "dashboard_arn" {
  description = "ARN of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.main.dashboard_arn
}

output "metric_stream_arn" {
  description = "ARN of the metric stream"
  value       = aws_cloudwatch_metric_stream.main.arn
}
