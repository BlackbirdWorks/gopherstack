resource "aws_route53_zone" "arc" {
  name = "arc-{{.Suffix}}.example.com"
}

resource "aws_route53recoveryreadiness_cell" "west" {
  cell_name = "arc_dns_west_{{.Suffix}}"
}

resource "aws_route53recoveryreadiness_resource_set" "dns" {
  resource_set_name = "arc_dns_{{.Suffix}}"
  resource_set_type = "AWS::Route53RecoveryReadiness::DNSTargetResource"

  resources {
    dns_target_resource {
      domain_name     = "app.arc-{{.Suffix}}.example.com"
      hosted_zone_arn = aws_route53_zone.arc.arn
      record_type     = "A"
      record_set_id   = "primary"

      target_resource {
        r53_resource {
          domain_name   = "app2.arc-{{.Suffix}}.example.com"
          record_set_id = "secondary"
        }
      }
    }

    readiness_scopes = [aws_route53recoveryreadiness_cell.west.arn]
  }

  tags = {
    env = "{{.Env}}"
  }
}
