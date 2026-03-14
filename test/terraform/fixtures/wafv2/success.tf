resource "aws_wafv2_web_acl" "example" {
  name  = "{{.WebACLName}}"
  scope = "REGIONAL"

  default_action {
    allow {}
  }

  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "{{.WebACLName}}"
    sampled_requests_enabled   = false
  }
}
