resource "aws_route53_resolver_endpoint" "this" {
  name               = "{{.EndpointName}}"
  direction          = "OUTBOUND"
  security_group_ids = ["sg-00000000"]

  ip_address {
    subnet_id = "subnet-00000001"
  }

  ip_address {
    subnet_id = "subnet-00000002"
  }
}

resource "aws_route53_resolver_rule" "this" {
  domain_name          = "example.internal."
  name                 = "{{.RuleName}}"
  rule_type            = "FORWARD"
  resolver_endpoint_id = aws_route53_resolver_endpoint.this.id

  target_ip {
    ip   = "10.0.1.10"
    port = 53
  }
}
