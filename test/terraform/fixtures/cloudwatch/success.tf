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
