# --- IoT -----------------------------------------------------------------

resource "aws_iot_thing_type" "example" {
  name = "mega-batch-22-thing-type"
}

resource "aws_iot_thing" "example" {
  name           = "mega-batch-22-thing"
  thing_type_name = aws_iot_thing_type.example.name
}

resource "aws_iot_thing_group" "example" {
  name = "mega-batch-22-thing-group"
}

resource "aws_iot_thing_group_membership" "example" {
  thing_name              = aws_iot_thing.example.name
  thing_group_name        = aws_iot_thing_group.example.name
  override_dynamic_group  = true
}

resource "aws_iot_certificate" "example" {
  active = true
}

resource "aws_iot_thing_principal_attachment" "example" {
  principal = aws_iot_certificate.example.arn
  thing     = aws_iot_thing.example.name
}

resource "aws_iot_ca_certificate" "example" {
  active                  = true
  allow_auto_registration = true
  certificate_mode        = "SNI_ONLY"
  ca_certificate_pem      = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAS6gAwIBAgIUV2VaBnegaBanana==\n-----END CERTIFICATE-----\n"
}

resource "aws_iot_policy" "example" {
  name = "mega-batch-22-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "iot:*"
      Resource = "*"
    }]
  })
}

resource "aws_iot_policy_attachment" "example" {
  policy = aws_iot_policy.example.name
  target = aws_iot_certificate.example.arn
}

resource "aws_iam_role" "iot" {
  name = "mega-batch-22-iot-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "iot.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iot_role_alias" "example" {
  alias    = "mega-batch-22-role-alias"
  role_arn = aws_iam_role.iot.arn
}

resource "aws_iot_logging_options" "example" {
  default_log_level = "WARN"
  role_arn          = aws_iam_role.iot.arn
}

resource "aws_iot_billing_group" "example" {
  name = "mega-batch-22-billing-group"
}

resource "aws_iot_indexing_configuration" "example" {
  thing_indexing_configuration {
    thing_indexing_mode = "REGISTRY"
  }

  thing_group_indexing_configuration {
    thing_group_indexing_mode = "OFF"
  }
}

resource "aws_iot_event_configurations" "example" {
  event_configurations = {
    "THING"                  = true
    "THING_GROUP"            = false
    "THING_TYPE"             = false
    "THING_GROUP_MEMBERSHIP" = false
    "THING_GROUP_HIERARCHY"  = false
    "THING_TYPE_ASSOCIATION" = false
    "JOB"                    = false
    "JOB_EXECUTION"          = false
    "POLICY"                 = false
    "CERTIFICATE"            = true
    "CA_CERTIFICATE"         = false
  }
}

resource "aws_iam_role" "iot_lambda" {
  name = "mega-batch-22-iot-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "iot_authorizer" {
  filename         = "{{.FunctionZip}}"
  function_name    = "mega-batch-22-iot-authorizer"
  role             = aws_iam_role.iot_lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
}

resource "aws_iot_authorizer" "example" {
  name                     = "mega-batch-22-authorizer"
  authorizer_function_arn  = aws_lambda_function.iot_authorizer.arn
  signing_disabled         = true
  status                   = "ACTIVE"
}

resource "aws_iot_provisioning_template" "example" {
  name                   = "mega-batch-22-provisioning-template"
  description            = "mega batch 22 provisioning template"
  provisioning_role_arn  = aws_iam_role.iot.arn
  enabled                = true

  template_body = jsonencode({
    Parameters = {
      SerialNumber = { Type = "String" }
    }
    Resources = {
      certificate = {
        Type = "AWS::IoT::Certificate"
        Properties = {
          CertificateId = { Ref = "AWS::IoT::Certificate::Id" }
          Status        = "Active"
        }
      }
      policy = {
        Type = "AWS::IoT::Policy"
        Properties = {
          PolicyName = aws_iot_policy.example.name
        }
      }
    }
  })
}

resource "aws_sns_topic" "iot" {
  name = "mega-batch-22-iot-topic"
}

resource "aws_iot_topic_rule" "example" {
  name        = "mega_batch_22_rule"
  description = "mega batch 22 topic rule"
  enabled     = true
  sql         = "SELECT * FROM 'mega-batch-22/topic'"
  sql_version = "2016-03-23"

  sns {
    message_format = "RAW"
    role_arn       = aws_iam_role.iot.arn
    target_arn     = aws_sns_topic.iot.arn
  }
}

resource "aws_vpc" "iot" {
  cidr_block = "10.150.0.0/16"

  tags = {
    Name = "mega-batch-22-vpc"
  }
}

resource "aws_subnet" "iot" {
  vpc_id     = aws_vpc.iot.id
  cidr_block = "10.150.1.0/24"

  tags = {
    Name = "mega-batch-22-subnet"
  }
}

resource "aws_security_group" "iot" {
  name   = "mega-batch-22-sg"
  vpc_id = aws_vpc.iot.id
}

resource "aws_iot_topic_rule_destination" "example" {
  vpc_configuration {
    role_arn        = aws_iam_role.iot.arn
    security_groups = [aws_security_group.iot.id]
    subnet_ids      = [aws_subnet.iot.id]
    vpc_id          = aws_vpc.iot.id
  }
}

resource "aws_iot_domain_configuration" "example" {
  name         = "mega-batch-22-domain-config"
  service_type = "DATA"
}

# --- SES -------------------------------------------------------------------

resource "aws_ses_domain_identity" "example" {
  domain = "mega-batch-22.example.com"
}

resource "aws_ses_domain_identity_verification" "example" {
  domain = aws_ses_domain_identity.example.domain
}

resource "aws_ses_domain_dkim" "example" {
  domain = aws_ses_domain_identity.example.domain
}

resource "aws_ses_domain_mail_from" "example" {
  domain           = aws_ses_domain_identity.example.domain
  mail_from_domain = "bounce.mega-batch-22.example.com"
}

resource "aws_ses_configuration_set" "example" {
  name = "mega-batch-22-config-set"
}

resource "aws_sns_topic" "ses" {
  name = "mega-batch-22-ses-topic"
}

resource "aws_ses_event_destination" "example" {
  name                   = "mega-batch-22-event-dest"
  configuration_set_name = aws_ses_configuration_set.example.name
  enabled                = true
  matching_types         = ["send", "bounce"]

  sns_destination {
    topic_arn = aws_sns_topic.ses.arn
  }
}

resource "aws_ses_identity_notification_topic" "example" {
  topic_arn         = aws_sns_topic.ses.arn
  notification_type = "Bounce"
  identity          = aws_ses_domain_identity.example.domain
}

resource "aws_ses_identity_policy" "example" {
  identity = aws_ses_domain_identity.example.domain
  name     = "mega-batch-22-identity-policy"

  policy = jsonencode({
    Id      = "mega-batch-22-policy"
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AuthorizeSend"
      Effect    = "Allow"
      Resource  = "arn:aws:ses:us-east-1:000000000000:identity/mega-batch-22.example.com"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = ["ses:SendEmail"]
    }]
  })
}

resource "aws_ses_receipt_rule_set" "example" {
  rule_set_name = "mega-batch-22-rule-set"
}

resource "aws_ses_active_receipt_rule_set" "example" {
  rule_set_name = aws_ses_receipt_rule_set.example.rule_set_name
}

resource "aws_ses_receipt_filter" "example" {
  name   = "mega-batch-22-receipt-filter"
  cidr   = "10.10.10.0/24"
  policy = "Block"
}

resource "aws_ses_receipt_rule" "example" {
  name          = "mega-batch-22-receipt-rule"
  rule_set_name = aws_ses_receipt_rule_set.example.rule_set_name
  recipients    = ["test@mega-batch-22.example.com"]
  enabled       = true
  scan_enabled  = true

  add_header_action {
    header_name  = "X-Mega-Batch"
    header_value = "22"
    position     = 1
  }

  stop_action {
    scope    = "RuleSet"
    position = 2
  }
}

resource "aws_ses_template" "example" {
  name    = "mega-batch-22-template"
  subject = "Mega Batch 22"
  html    = "<h1>Mega Batch 22</h1>"
  text    = "Mega Batch 22"
}
