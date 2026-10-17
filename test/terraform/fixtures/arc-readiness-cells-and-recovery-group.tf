resource "aws_route53recoveryreadiness_cell" "west_az" {
  cell_name = "arc_west_az_{{.Suffix}}"
}

resource "aws_route53recoveryreadiness_cell" "west" {
  cell_name = "arc_west_{{.Suffix}}"
  cells     = [aws_route53recoveryreadiness_cell.west_az.arn]

  tags = {
    env = "{{.Env}}"
  }
}

resource "aws_route53recoveryreadiness_cell" "east" {
  cell_name = "arc_east_{{.Suffix}}"

  tags = {
    env = "{{.Env}}"
  }
}

resource "aws_route53recoveryreadiness_recovery_group" "arc" {
  recovery_group_name = "arc_group_{{.Suffix}}"
  cells = [
    aws_route53recoveryreadiness_cell.west.arn,
    aws_route53recoveryreadiness_cell.east.arn,
  ]

  tags = {
    env = "{{.Env}}"
  }
}
