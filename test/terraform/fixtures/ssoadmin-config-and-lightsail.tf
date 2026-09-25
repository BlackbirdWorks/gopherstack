# --- Lightsail ---

resource "aws_lightsail_instance" "example" {
  name              = "mega-batch-30-instance"
  availability_zone = "us-east-1a"
  blueprint_id      = "amazon_linux_2023"
  bundle_id         = "nano_3_0"
}

resource "aws_lightsail_instance_public_ports" "example" {
  instance_name = aws_lightsail_instance.example.name

  port_info {
    protocol  = "tcp"
    from_port = 22
    to_port   = 22
  }
}

resource "aws_lightsail_disk" "example" {
  name              = "mega-batch-30-disk"
  availability_zone = "us-east-1a"
  size_in_gb        = 8
}

resource "aws_lightsail_disk_attachment" "example" {
  disk_name     = aws_lightsail_disk.example.name
  instance_name = aws_lightsail_instance.example.name
  disk_path     = "/dev/xvdf"
}

resource "aws_lightsail_static_ip" "example" {
  name = "mega-batch-30-static-ip"
}

resource "aws_lightsail_static_ip_attachment" "example" {
  static_ip_name = aws_lightsail_static_ip.example.name
  instance_name  = aws_lightsail_instance.example.name
}

resource "aws_lightsail_bucket" "example" {
  name      = "mega-batch-30-bucket"
  bundle_id = "small_1_0"
}

resource "aws_lightsail_bucket_access_key" "example" {
  bucket_name = aws_lightsail_bucket.example.id
}

resource "aws_lightsail_container_service" "example" {
  name        = "mega-batch-30-container"
  power       = "nano"
  scale       = 1
  is_disabled = false
}

resource "aws_lightsail_container_service_deployment_version" "example" {
  service_name = aws_lightsail_container_service.example.name

  container {
    container_name = "app"
    image          = "nginx:latest"

    ports = {
      80 = "HTTP"
    }
  }

  public_endpoint {
    container_name = "app"
    container_port = 80

    health_check {
      healthy_threshold   = 2
      unhealthy_threshold = 2
      timeout_seconds     = 2
      interval_seconds    = 5
      path                = "/"
      success_codes       = "200-499"
    }
  }
}

resource "aws_lightsail_bucket_resource_access" "example" {
  bucket_name   = aws_lightsail_bucket.example.id
  resource_name = aws_lightsail_container_service.example.name
}

resource "aws_lightsail_distribution" "example" {
  name      = "mega-batch-30-distribution"
  bundle_id = "small_1_0"

  origin {
    name            = aws_lightsail_instance.example.name
    region_name     = "us-east-1"
    protocol_policy = "http-only"
  }

  default_cache_behavior {
    behavior = "cache"
  }
}

resource "aws_lightsail_certificate" "example" {
  name        = "mega-batch-30-cert"
  domain_name = "example-mega-batch-30.com"
}

resource "aws_lightsail_domain" "example" {
  domain_name = "mega-batch-30-domain.com"
}

resource "aws_lightsail_domain_entry" "example" {
  domain_name = aws_lightsail_domain.example.domain_name
  name        = "www"
  type        = "A"
  target      = "127.0.0.1"
}

resource "aws_lightsail_database" "example" {
  relational_database_name = "mega-batch-30-db"
  availability_zone         = "us-east-1a"
  master_database_name      = "megabatch30"
  master_username           = "megabatch30admin"
  master_password            = "MegaBatch30Password!"
  blueprint_id               = "mysql_8_0"
  bundle_id                  = "micro_2_0"
  skip_final_snapshot        = true
}

resource "aws_lightsail_lb" "example" {
  name              = "mega-batch-30-lb"
  health_check_path = "/"
  instance_port     = 80
}

resource "aws_lightsail_lb_attachment" "example" {
  lb_name       = aws_lightsail_lb.example.name
  instance_name = aws_lightsail_instance.example.name
}

resource "aws_lightsail_lb_https_redirection_policy" "example" {
  lb_name = aws_lightsail_lb.example.name
  enabled = true
}

resource "aws_lightsail_lb_stickiness_policy" "example" {
  lb_name         = aws_lightsail_lb.example.name
  cookie_duration = 300
  enabled         = true
}

resource "aws_lightsail_lb_certificate" "example" {
  name        = "mega-batch-30-lb-cert"
  lb_name     = aws_lightsail_lb.example.name
  domain_name = "example-mega-batch-30-lb.com"
}

resource "aws_lightsail_lb_certificate_attachment" "example" {
  lb_name          = aws_lightsail_lb.example.name
  certificate_name = aws_lightsail_lb_certificate.example.name
}

# --- SSO Admin ---

data "aws_ssoadmin_instances" "this" {}

resource "aws_ssoadmin_permission_set" "example" {
  name         = "mega-batch-30-permission-set"
  instance_arn = tolist(data.aws_ssoadmin_instances.this.arns)[0]
}

resource "aws_ssoadmin_permission_set_inline_policy" "example" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.example.arn

  inline_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:ListAllMyBuckets"]
      Resource = "*"
    }]
  })
}

resource "aws_ssoadmin_managed_policy_attachment" "example" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.example.arn
  managed_policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_ssoadmin_customer_managed_policy_attachment" "example" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.example.arn

  customer_managed_policy_reference {
    name = "mega-batch-30-cmp"
    path = "/"
  }
}

resource "aws_ssoadmin_permissions_boundary_attachment" "example" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.example.arn

  permissions_boundary {
    managed_policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
  }
}

resource "aws_identitystore_group" "example" {
  identity_store_id = tolist(data.aws_ssoadmin_instances.this.identity_store_ids)[0]
  display_name      = "mega-batch-30-group"
  description       = "Mega Batch 30 group"
}

resource "aws_ssoadmin_account_assignment" "example" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.example.arn

  principal_id   = aws_identitystore_group.example.group_id
  principal_type = "GROUP"

  target_id   = "000000000000"
  target_type = "AWS_ACCOUNT"
}

resource "aws_ssoadmin_instance_access_control_attributes" "example" {
  instance_arn = tolist(data.aws_ssoadmin_instances.this.arns)[0]

  attribute {
    key = "department"

    value {
      source = ["$${path:department}"]
    }
  }
}

resource "aws_ssoadmin_application" "example" {
  name                     = "mega-batch-30-application"
  instance_arn             = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  application_provider_arn = "arn:aws:sso::aws:applicationProvider/custom"
}

resource "aws_ssoadmin_application_access_scope" "example" {
  application_arn = aws_ssoadmin_application.example.application_arn
  scope           = "sso:account:access"
}

resource "aws_ssoadmin_application_assignment_configuration" "example" {
  application_arn = aws_ssoadmin_application.example.application_arn
  assignment_required = true
}

resource "aws_ssoadmin_application_assignment" "example" {
  application_arn = aws_ssoadmin_application.example.application_arn
  principal_id    = aws_identitystore_group.example.group_id
  principal_type  = "GROUP"
}

resource "aws_ssoadmin_trusted_token_issuer" "example" {
  name          = "mega-batch-30-tti"
  instance_arn  = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  trusted_token_issuer_type = "OIDC_JWT"

  trusted_token_issuer_configuration {
    oidc_jwt_configuration {
      claim_attribute_path      = "email"
      identity_store_attribute_path = "emails.value"
      issuer_url                 = "https://mega-batch-30.example.com"
      jwks_retrieval_option      = "OPEN_ID_DISCOVERY"
    }
  }
}

# --- AWS Config ---

resource "aws_iam_role" "config" {
  name = "mega-batch-30-config-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "config.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "config" {
  bucket = "mega-batch-30-config-bucket"
}

resource "aws_config_configuration_recorder" "example" {
  name     = "mega-batch-30-recorder"
  role_arn = aws_iam_role.config.arn
}

resource "aws_config_delivery_channel" "example" {
  name           = "mega-batch-30-channel"
  s3_bucket_name = aws_s3_bucket.config.bucket

  depends_on = [aws_config_configuration_recorder.example]
}

resource "aws_config_configuration_recorder_status" "example" {
  name       = aws_config_configuration_recorder.example.name
  is_enabled = true

  depends_on = [aws_config_delivery_channel.example]
}

resource "aws_config_config_rule" "example" {
  name = "mega-batch-30-config-rule"

  source {
    owner             = "AWS"
    source_identifier = "S3_BUCKET_VERSIONING_ENABLED"
  }

  depends_on = [aws_config_configuration_recorder.example]
}

resource "aws_config_remediation_configuration" "example" {
  config_rule_name = aws_config_config_rule.example.name
  resource_type    = "AWS::S3::Bucket"
  target_id        = "AWS-EnableS3BucketEncryption"
  target_type      = "SSM_DOCUMENT"
  target_version   = "1"

  parameter {
    name           = "AutomationAssumeRole"
    static_value   = aws_iam_role.config.arn
  }
}

resource "aws_config_retention_configuration" "example" {
  retention_period_in_days = 90
}

resource "aws_config_configuration_aggregator" "example" {
  name = "mega-batch-30-aggregator"

  account_aggregation_source {
    account_ids = ["000000000000"]
    all_regions = true
  }
}

resource "aws_config_aggregate_authorization" "example" {
  account_id = "111111111111"
  region     = "us-west-2"
}

resource "aws_config_conformance_pack" "example" {
  name = "mega-batch-30-conformance-pack"

  template_body = <<EOT
Resources:
  ConfigRule:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: mega-batch-30-cp-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED
EOT

  depends_on = [aws_config_configuration_recorder.example]
}

resource "aws_config_organization_custom_rule" "example" {
  name                = "mega-batch-30-org-custom-rule"
  lambda_function_arn = aws_lambda_function.config_custom_rule.arn
  trigger_types       = ["ConfigurationItemChangeNotification"]
}

resource "aws_config_organization_managed_rule" "example" {
  name            = "mega-batch-30-org-managed-rule"
  rule_identifier = "S3_BUCKET_VERSIONING_ENABLED"
}

resource "aws_config_organization_conformance_pack" "example" {
  name = "mega-batch-30-org-conformance-pack"

  template_body = <<EOT
Resources:
  ConfigRule:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: mega-batch-30-org-cp-rule
      Source:
        Owner: AWS
        SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED
EOT
}

resource "aws_iam_role" "config_lambda" {
  name = "mega-batch-30-config-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "config_custom_rule" {
  function_name    = "mega-batch-30-config-custom-rule"
  role             = aws_iam_role.config_lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  filename         = "{{.FunctionZip}}"
  source_code_hash = filebase64sha256("{{.FunctionZip}}")
}
