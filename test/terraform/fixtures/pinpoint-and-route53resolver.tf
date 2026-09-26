resource "aws_pinpoint_app" "pnrr" {
  name = "pnrr-app"
}

resource "aws_pinpoint_adm_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  client_id      = "pnrr-adm-client-id"
  client_secret  = "pnrr-adm-client-secret"
}

resource "aws_pinpoint_apns_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nPNRRFAKECERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nPNRRFAKEKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_sandbox_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nPNRRFAKESANDBOXCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nPNRRFAKESANDBOXKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_voip_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nPNRRFAKEVOIPCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nPNRRFAKEVOIPKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_voip_sandbox_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nPNRRFAKEVOIPSANDBOXCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nPNRRFAKEVOIPSANDBOXKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_baidu_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  api_key        = "pnrr-baidu-api-key"
  secret_key     = "pnrr-baidu-secret-key"
}

resource "aws_iam_role" "pnrr_pinpoint_email" {
  name = "pnrr-pinpoint-email-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pinpoint.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_pinpoint_email_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  from_address   = "test@pnrr.example.com"
  identity       = "arn:aws:ses:us-east-1:000000000000:identity/pnrr.example.com"
  role_arn       = aws_iam_role.pnrr_pinpoint_email.arn
}

resource "aws_pinpoint_email_template" "pnrr" {
  template_name = "pnrr-email-template"

  email_template {
    subject   = "Pnrr Subject"
    html_part = "<html><body>Pnrr</body></html>"
    text_part = "Pnrr"
  }
}

resource "aws_iam_role" "pnrr_pinpoint_event_stream" {
  name = "pnrr-pinpoint-event-stream-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pinpoint.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kinesis_stream" "pnrr" {
  name             = "pnrr-event-stream"
  shard_count      = 1
  retention_period = 24
}

resource "aws_pinpoint_event_stream" "pnrr" {
  application_id         = aws_pinpoint_app.pnrr.application_id
  destination_stream_arn = aws_kinesis_stream.pnrr.arn
  role_arn               = aws_iam_role.pnrr_pinpoint_event_stream.arn
}

resource "aws_pinpoint_gcm_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  api_key        = "pnrr-gcm-api-key"
}

resource "aws_pinpoint_sms_channel" "pnrr" {
  application_id = aws_pinpoint_app.pnrr.application_id
  enabled        = true
  sender_id      = "MEGA28"
}

resource "aws_vpc" "pnrr" {
  cidr_block = "10.170.0.0/16"

  tags = {
    Name = "pnrr-vpc"
  }
}

resource "aws_route53_resolver_config" "pnrr" {
  resource_id              = aws_vpc.pnrr.id
  autodefined_reverse_flag = "DISABLE"
}

resource "aws_route53_resolver_dnssec_config" "pnrr" {
  resource_id = aws_vpc.pnrr.id
}

resource "aws_route53_resolver_firewall_config" "pnrr" {
  resource_id        = aws_vpc.pnrr.id
  firewall_fail_open = "ENABLED"
}

resource "aws_route53_resolver_firewall_domain_list" "pnrr" {
  name    = "pnrr-firewall-domain-list"
  domains = ["example.com", "test.example.com"]
}

resource "aws_route53_resolver_firewall_rule_group" "pnrr" {
  name = "pnrr-firewall-rule-group"
}

resource "aws_route53_resolver_firewall_rule" "pnrr" {
  name                    = "pnrr-firewall-rule"
  action                  = "BLOCK"
  block_response          = "NXDOMAIN"
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.pnrr.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.pnrr.id
  priority                = 100
}

resource "aws_route53_resolver_firewall_rule_group_association" "pnrr" {
  name                   = "pnrr-firewall-rule-group-association"
  firewall_rule_group_id = aws_route53_resolver_firewall_rule_group.pnrr.id
  priority               = 101
  vpc_id                 = aws_vpc.pnrr.id
}

resource "aws_cloudwatch_log_group" "pnrr" {
  name = "/pnrr/resolver-query-log"
}

resource "aws_route53_resolver_query_log_config" "pnrr" {
  name            = "pnrr-query-log-config"
  destination_arn = aws_cloudwatch_log_group.pnrr.arn
}

resource "aws_route53_resolver_query_log_config_association" "pnrr" {
  resolver_query_log_config_id = aws_route53_resolver_query_log_config.pnrr.id
  resource_id                  = aws_vpc.pnrr.id
}

resource "aws_route53_resolver_rule" "pnrr" {
  domain_name = "pnrr.example.com"
  rule_type   = "FORWARD"

  target_ip {
    ip = "10.170.100.5"
  }
}

resource "aws_route53_resolver_rule_association" "pnrr" {
  resolver_rule_id = aws_route53_resolver_rule.pnrr.id
  vpc_id           = aws_vpc.pnrr.id
}
