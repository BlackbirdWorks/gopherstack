resource "aws_pinpoint_app" "mb28" {
  name = "mega-batch-28-app"
}

resource "aws_pinpoint_adm_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  client_id      = "mega-batch-28-adm-client-id"
  client_secret  = "mega-batch-28-adm-client-secret"
}

resource "aws_pinpoint_apns_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nMEGABATCH28FAKECERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nMEGABATCH28FAKEKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_sandbox_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nMEGABATCH28FAKESANDBOXCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nMEGABATCH28FAKESANDBOXKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_voip_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nMEGABATCH28FAKEVOIPCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nMEGABATCH28FAKEVOIPKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_apns_voip_sandbox_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  certificate    = "-----BEGIN CERTIFICATE-----\nMEGABATCH28FAKEVOIPSANDBOXCERT\n-----END CERTIFICATE-----"
  private_key    = "-----BEGIN PRIVATE KEY-----\nMEGABATCH28FAKEVOIPSANDBOXKEY\n-----END PRIVATE KEY-----"
}

resource "aws_pinpoint_baidu_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  api_key        = "mega-batch-28-baidu-api-key"
  secret_key     = "mega-batch-28-baidu-secret-key"
}

resource "aws_iam_role" "mb28_pinpoint_email" {
  name = "mega-batch-28-pinpoint-email-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pinpoint.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_pinpoint_email_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  from_address   = "test@mega-batch-28.example.com"
  identity       = "arn:aws:ses:us-east-1:000000000000:identity/mega-batch-28.example.com"
  role_arn       = aws_iam_role.mb28_pinpoint_email.arn
}

resource "aws_pinpoint_email_template" "mb28" {
  template_name = "mega-batch-28-email-template"

  email_template {
    subject   = "Mega Batch 28 Subject"
    html_part = "<html><body>Mega Batch 28</body></html>"
    text_part = "Mega Batch 28"
  }
}

resource "aws_iam_role" "mb28_pinpoint_event_stream" {
  name = "mega-batch-28-pinpoint-event-stream-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pinpoint.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kinesis_stream" "mb28" {
  name             = "mega-batch-28-event-stream"
  shard_count      = 1
  retention_period = 24
}

resource "aws_pinpoint_event_stream" "mb28" {
  application_id         = aws_pinpoint_app.mb28.application_id
  destination_stream_arn = aws_kinesis_stream.mb28.arn
  role_arn                = aws_iam_role.mb28_pinpoint_event_stream.arn
}

resource "aws_pinpoint_gcm_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  api_key        = "mega-batch-28-gcm-api-key"
}

resource "aws_pinpoint_sms_channel" "mb28" {
  application_id = aws_pinpoint_app.mb28.application_id
  enabled        = true
  sender_id      = "MEGA28"
}

resource "aws_vpc" "mb28" {
  cidr_block = "10.170.0.0/16"

  tags = {
    Name = "mega-batch-28-vpc"
  }
}

resource "aws_route53_resolver_config" "mb28" {
  resource_id                   = aws_vpc.mb28.id
  autodefined_reverse_flag = "DISABLE"
}

resource "aws_route53_resolver_dnssec_config" "mb28" {
  resource_id = aws_vpc.mb28.id
}

resource "aws_route53_resolver_firewall_config" "mb28" {
  resource_id         = aws_vpc.mb28.id
  firewall_fail_open = "ENABLED"
}

resource "aws_route53_resolver_firewall_domain_list" "mb28" {
  name    = "mega-batch-28-firewall-domain-list"
  domains = ["example.com", "test.example.com"]
}

resource "aws_route53_resolver_firewall_rule_group" "mb28" {
  name = "mega-batch-28-firewall-rule-group"
}

resource "aws_route53_resolver_firewall_rule" "mb28" {
  name                    = "mega-batch-28-firewall-rule"
  action                  = "BLOCK"
  block_response          = "NXDOMAIN"
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.mb28.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.mb28.id
  priority                = 100
}

resource "aws_route53_resolver_firewall_rule_group_association" "mb28" {
  name                    = "mega-batch-28-firewall-rule-group-association"
  firewall_rule_group_id = aws_route53_resolver_firewall_rule_group.mb28.id
  priority                = 101
  vpc_id                  = aws_vpc.mb28.id
}

resource "aws_cloudwatch_log_group" "mb28" {
  name = "/mega-batch-28/resolver-query-log"
}

resource "aws_route53_resolver_query_log_config" "mb28" {
  name            = "mega-batch-28-query-log-config"
  destination_arn = aws_cloudwatch_log_group.mb28.arn
}

resource "aws_route53_resolver_query_log_config_association" "mb28" {
  resolver_query_log_config_id = aws_route53_resolver_query_log_config.mb28.id
  resource_id                   = aws_vpc.mb28.id
}

resource "aws_route53_resolver_rule" "mb28" {
  domain_name = "mega-batch-28.example.com"
  rule_type   = "FORWARD"

  target_ip {
    ip = "10.170.100.5"
  }
}

resource "aws_route53_resolver_rule_association" "mb28" {
  resolver_rule_id = aws_route53_resolver_rule.mb28.id
  vpc_id           = aws_vpc.mb28.id
}
