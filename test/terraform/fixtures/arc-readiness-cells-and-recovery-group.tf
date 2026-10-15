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
