resource "aws_route53_resolver_rule" "this" {
  domain_name          = "example.internal."
  name                 = "{{.RuleName}}"
  rule_type            = "FORWARD"
  resolver_endpoint_id = "rslvr-out-{{.EndpointID}}"

  target_ip {
    ip   = "10.0.1.10"
    port = 53
  }
}
