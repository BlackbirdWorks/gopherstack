resource "aws_waf_ipset" "this" {
  name = "{{.IPSetName}}"

  ip_set_descriptors {
    type  = "IPV4"
    value = "10.0.0.0/8"
  }

  ip_set_descriptors {
    type  = "IPV4"
    value = "192.168.0.0/16"
  }
}

resource "aws_waf_rule" "this" {
  name        = "{{.RuleName}}"
  metric_name = "{{.MetricName}}"

  predicates {
    data_id = aws_waf_ipset.this.id
    negated = false
    type    = "IPMatch"
  }
}
