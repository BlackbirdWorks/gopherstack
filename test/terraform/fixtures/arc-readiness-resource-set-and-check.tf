resource "aws_sqs_queue" "arc" {
  name = "arc-orders-{{.Suffix}}"
}

resource "aws_route53recoveryreadiness_cell" "west" {
  cell_name = "arc_west_{{.Suffix}}"
}

resource "aws_route53recoveryreadiness_cell" "east" {
  cell_name = "arc_east_{{.Suffix}}"
}

resource "aws_route53recoveryreadiness_recovery_group" "arc" {
  recovery_group_name = "arc_group_{{.Suffix}}"
  cells = [
    aws_route53recoveryreadiness_cell.west.arn,
    aws_route53recoveryreadiness_cell.east.arn,
  ]
}

resource "aws_route53recoveryreadiness_resource_set" "arc" {
  resource_set_name = "arc_queues_{{.Suffix}}"
  resource_set_type = "AWS::SQS::Queue"

  resources {
    resource_arn     = aws_sqs_queue.arc.arn
    readiness_scopes = [aws_route53recoveryreadiness_cell.west.arn]
  }
}

resource "aws_route53recoveryreadiness_readiness_check" "arc" {
  readiness_check_name = "arc_check_{{.Suffix}}"
  resource_set_name    = aws_route53recoveryreadiness_resource_set.arc.resource_set_name
}
