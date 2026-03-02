resource "aws_iam_role" "scheduler" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_scheduler_schedule" "this" {
  name = "{{.ScheduleName}}"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(5 minutes)"

  target {
    arn      = "arn:aws:sqs:us-east-1:000000000000:tf-sched-queue"
    role_arn = aws_iam_role.scheduler.arn
  }
}
