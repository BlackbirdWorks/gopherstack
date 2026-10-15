resource "aws_route53recoverycontrolconfig_cluster" "arc" {
  name = "arc-cluster-{{.Suffix}}"
}

resource "aws_route53recoverycontrolconfig_control_panel" "arc" {
  name        = "arc-panel-{{.Suffix}}"
  cluster_arn = aws_route53recoverycontrolconfig_cluster.arc.arn
}

resource "aws_route53recoverycontrolconfig_routing_control" "primary" {
  name              = "arc-primary-{{.Suffix}}"
  cluster_arn       = aws_route53recoverycontrolconfig_cluster.arc.arn
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
}

resource "aws_route53recoverycontrolconfig_routing_control" "secondary" {
  name              = "arc-secondary-{{.Suffix}}"
  cluster_arn       = aws_route53recoverycontrolconfig_cluster.arc.arn
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
}

resource "aws_route53recoverycontrolconfig_safety_rule" "assertion" {
  name              = "arc-at-least-one-{{.Suffix}}"
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
  wait_period_ms    = 5000

  asserted_controls = [
    aws_route53recoverycontrolconfig_routing_control.primary.arn,
    aws_route53recoverycontrolconfig_routing_control.secondary.arn,
  ]

  rule_config {
    inverted  = false
    threshold = 1
    type      = "ATLEAST"
  }
}

resource "aws_route53recoverycontrolconfig_safety_rule" "gating" {
  name              = "arc-gate-secondary-{{.Suffix}}"
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
  wait_period_ms    = 5000

  gating_controls = [aws_route53recoverycontrolconfig_routing_control.primary.arn]
  target_controls = [aws_route53recoverycontrolconfig_routing_control.secondary.arn]

  rule_config {
    inverted  = false
    threshold = 1
    type      = "ATLEAST"
  }
}

resource "aws_route53_health_check" "arc" {
  type                = "RECOVERY_CONTROL"
  routing_control_arn = aws_route53recoverycontrolconfig_routing_control.primary.arn
}
