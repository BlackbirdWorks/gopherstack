resource "aws_vpc" "arczs" {
  cidr_block = "{{.VPCCidr}}"
}

resource "aws_subnet" "arczs_a" {
  vpc_id            = aws_vpc.arczs.id
  cidr_block        = "{{.SubnetCidrA}}"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "arczs_b" {
  vpc_id            = aws_vpc.arczs.id
  cidr_block        = "{{.SubnetCidrB}}"
  availability_zone = "us-east-1b"
}

resource "aws_lb" "arczs" {
  name               = "arczs-{{.Suffix}}"
  internal           = true
  load_balancer_type = "application"
  subnets            = [aws_subnet.arczs_a.id, aws_subnet.arczs_b.id]

  drop_invalid_header_fields = true
}

resource "aws_cloudwatch_metric_alarm" "arczs_outcome" {
  alarm_name          = "arczs-outcome-{{.Suffix}}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TargetResponseTime"
  namespace           = "AWS/ApplicationELB"
  period              = 60
  statistic           = "Average"
  threshold           = 1
}

resource "aws_cloudwatch_metric_alarm" "arczs_blocking" {
  alarm_name          = "arczs-blocking-{{.Suffix}}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "HTTPCode_Target_5XX_Count"
  namespace           = "AWS/ApplicationELB"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
}

resource "aws_arczonalshift_zonal_autoshift_configuration" "arczs" {
  resource_arn           = aws_lb.arczs.arn
  zonal_autoshift_status = "{{.Status}}"
  blocked_windows        = ["{{.BlockedWindow}}"]
  blocked_dates          = ["2030-12-25"]

  outcome_alarms {
    alarm_identifier = aws_cloudwatch_metric_alarm.arczs_outcome.arn
    type             = "CLOUDWATCH"
  }

  blocking_alarms {
    alarm_identifier = aws_cloudwatch_metric_alarm.arczs_blocking.arn
    type             = "CLOUDWATCH"
  }
}
