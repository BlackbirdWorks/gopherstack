resource "aws_cloudwatch_metric_alarm" "this" {
  alarm_name          = "{{.AlarmName}}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 80
}

resource "aws_cloudwatch_dashboard" "this" {
  dashboard_name = "{{.AlarmName}}-dashboard"
  dashboard_body = jsonencode({
    widgets = [
      {
        type       = "metric"
        x          = 0
        y          = 0
        width      = 12
        height     = 6
        properties = {}
      }
    ]
  })
}
