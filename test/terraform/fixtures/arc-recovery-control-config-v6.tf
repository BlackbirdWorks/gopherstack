resource "aws_route53recoverycontrolconfig_cluster" "arc" {
  name         = "arc-cluster-{{.Suffix}}"
  network_type = "IPV4"

  tags = {
    env = "{{.Env}}"
  }
}

resource "aws_route53recoverycontrolconfig_control_panel" "arc" {
  name        = "{{.PanelName}}"
  cluster_arn = aws_route53recoverycontrolconfig_cluster.arc.arn

  tags = {
    env = "{{.Env}}"
  }
}

resource "aws_route53recoverycontrolconfig_routing_control" "primary" {
  name              = "arc-primary-{{.Suffix}}"
  cluster_arn       = aws_route53recoverycontrolconfig_cluster.arc.arn
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
}

resource "aws_route53recoverycontrolconfig_routing_control" "secondary" {
  name              = "{{.SecondaryName}}"
  cluster_arn       = aws_route53recoverycontrolconfig_cluster.arc.arn
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
}

resource "aws_route53recoverycontrolconfig_safety_rule" "assertion" {
  name              = "{{.AssertionName}}"
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
  wait_period_ms    = {{.WaitMs}}

  tags = {
    env = "{{.Env}}"
  }

  asserted_controls = [
    aws_route53recoverycontrolconfig_routing_control.primary.arn,
    aws_route53recoverycontrolconfig_routing_control.secondary.arn,
  ]

  rule_config {
    inverted  = false
    threshold = {{.Threshold}}
    type      = "ATLEAST"
  }
}

resource "aws_route53recoverycontrolconfig_safety_rule" "gating" {
  name              = "{{.GatingName}}"
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
  wait_period_ms    = {{.WaitMs}}

  tags = {
    env = "{{.Env}}"
  }

  gating_controls = [aws_route53recoverycontrolconfig_routing_control.primary.arn]
  target_controls = [aws_route53recoverycontrolconfig_routing_control.secondary.arn]

  rule_config {
    inverted  = true
    threshold = 1
    type      = "OR"
  }
}

resource "aws_route53_health_check" "arc" {
  type                = "RECOVERY_CONTROL"
  routing_control_arn = aws_route53recoverycontrolconfig_routing_control.primary.arn
}
