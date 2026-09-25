##############################################################################
# Cognito IDP: identity provider, resource server, user pool domain, UI
# customization, and risk configuration on a shared user pool.
##############################################################################

resource "aws_cognito_user_pool" "cecw" {
  name = "cecw-pool"
}

resource "aws_cognito_user_pool_client" "cecw" {
  name         = "cecw-client"
  user_pool_id = aws_cognito_user_pool.cecw.id
}

resource "aws_cognito_identity_provider" "cecw" {
  user_pool_id  = aws_cognito_user_pool.cecw.id
  provider_name = "CecwGoogle"
  provider_type = "Google"

  provider_details = {
    client_id        = "cecw-client-id"
    client_secret    = "cecw-client-secret"
    authorize_scopes = "email profile"
  }

  attribute_mapping = {
    email = "email"
  }
}

resource "aws_cognito_resource_server" "cecw" {
  user_pool_id = aws_cognito_user_pool.cecw.id
  identifier   = "cecw-api"
  name         = "cecw-resource-server"

  scope {
    scope_name        = "read"
    scope_description = "read access"
  }

  scope {
    scope_name        = "write"
    scope_description = "write access"
  }
}

resource "aws_cognito_user_pool_domain" "cecw" {
  domain       = "cecw-domain"
  user_pool_id = aws_cognito_user_pool.cecw.id
}

resource "aws_cognito_user_pool_ui_customization" "cecw" {
  user_pool_id = aws_cognito_user_pool.cecw.id
  client_id    = "ALL"
  css          = ".label-customizable {font-weight: 400;}"

  depends_on = [aws_cognito_user_pool_domain.cecw]
}

resource "aws_cognito_risk_configuration" "cecw" {
  user_pool_id = aws_cognito_user_pool.cecw.id

  compromised_credentials_risk_configuration {
    actions {
      event_action = "BLOCK"
    }
  }
}

##############################################################################
# ECS: account setting default, capacity provider (backed by an ASG),
# cluster/capacity-provider association, a tag, and a task set on an
# EXTERNAL-controller service.
##############################################################################

resource "aws_ecs_account_setting_default" "cecw" {
  name  = "containerInsights"
  value = "enabled"
}

resource "aws_launch_template" "cecw_ecs" {
  name_prefix   = "cecw-lt-"
  image_id      = "ami-0c55b159cbfafe1f0"
  instance_type = "t2.micro"
}

resource "aws_autoscaling_group" "cecw_ecs" {
  name               = "cecw-ecs-asg"
  min_size           = 1
  max_size           = 3
  desired_capacity   = 1
  availability_zones = ["us-east-1a"]

  launch_template {
    id      = aws_launch_template.cecw_ecs.id
    version = "$Latest"
  }
}

resource "aws_ecs_capacity_provider" "cecw" {
  name = "cecw-cp"

  auto_scaling_group_provider {
    auto_scaling_group_arn = aws_autoscaling_group.cecw_ecs.arn

    managed_scaling {
      status          = "ENABLED"
      target_capacity = 75
    }
  }
}

resource "aws_ecs_cluster" "cecw" {
  name = "cecw-cluster"
}

resource "aws_ecs_cluster_capacity_providers" "cecw" {
  cluster_name       = aws_ecs_cluster.cecw.name
  capacity_providers = [aws_ecs_capacity_provider.cecw.name]

  default_capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.cecw.name
    weight            = 1
  }
}

resource "aws_ecs_tag" "cecw" {
  resource_arn = aws_ecs_cluster.cecw.arn
  key          = "cecw-key"
  value        = "cecw-value"
}

resource "aws_ecs_task_definition" "cecw" {
  family = "cecw-task"

  container_definitions = jsonencode([
    {
      name      = "nginx"
      image     = "nginx:latest"
      essential = true
    }
  ])
}

resource "aws_ecs_service" "cecw" {
  name                  = "cecw-service"
  cluster               = aws_ecs_cluster.cecw.arn
  desired_count         = 1
  wait_for_steady_state = false

  deployment_controller {
    type = "EXTERNAL"
  }
}

resource "aws_ecs_task_set" "cecw" {
  cluster         = aws_ecs_cluster.cecw.arn
  service         = aws_ecs_service.cecw.id
  task_definition = aws_ecs_task_definition.cecw.arn
}

##############################################################################
# CloudFormation: self-managed StackSet with a singular stack-set instance
# and a bulk stack-instances deployment target, plus a private resource type
# registration.
##############################################################################

resource "aws_iam_role" "cecw_cfn_admin" {
  name = "cecw-cfn-admin-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudformation.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudformation_stack_set" "cecw" {
  name                    = "cecw-stackset"
  administration_role_arn = aws_iam_role.cecw_cfn_admin.arn
  execution_role_name     = "cecw-cfn-exec-role"

  template_body = <<TEMPLATE
{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    }
  }
}
TEMPLATE
}

resource "aws_cloudformation_stack_set_instance" "cecw" {
  stack_set_name = aws_cloudformation_stack_set.cecw.name
  account_id     = "000000000000"
  region         = "us-east-1"
}

# Separate stack set: the provider reads back every instance of a stack set
# as the resource's accounts/regions, so sharing one with the singular
# stack_set_instance above would never re-plan empty.
resource "aws_cloudformation_stack_set" "cecw_bulk" {
  name                    = "cecw-stackset-bulk"
  administration_role_arn = aws_iam_role.cecw_cfn_admin.arn
  execution_role_name     = "cecw-cfn-exec-role"
  template_body           = aws_cloudformation_stack_set.cecw.template_body
}

resource "aws_cloudformation_stack_instances" "cecw" {
  stack_set_name = aws_cloudformation_stack_set.cecw_bulk.name
  regions        = ["us-west-2"]

  # Without deployment_targets the provider replaces `accounts` with its own
  # caller account id, which is "" under skip_requesting_account_id.
  deployment_targets {
    accounts = ["000000000000"]
  }
}

resource "aws_cloudformation_type" "cecw" {
  type                   = "RESOURCE"
  type_name              = "Cecw::Example::Resource"
  schema_handler_package = "s3://cecw-bucket/schema-handler.zip"
}

##############################################################################
# CodeBuild: compute fleet, report group with a resource policy, a source
# credential, and a GitHub-sourced project with a webhook.
##############################################################################

resource "aws_codebuild_fleet" "cecw" {
  name             = "cecw-fleet"
  base_capacity    = 1
  compute_type     = "BUILD_GENERAL1_SMALL"
  environment_type = "LINUX_CONTAINER"
}

resource "aws_codebuild_report_group" "cecw" {
  name = "cecw-report-group"
  type = "TEST"

  export_config {
    type = "NO_EXPORT"
  }
}

resource "aws_codebuild_resource_policy" "cecw" {
  resource_arn = aws_codebuild_report_group.cecw.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "cecw-policy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "codebuild:BatchGetReportGroups"
      Resource  = aws_codebuild_report_group.cecw.arn
    }]
  })
}

resource "aws_codebuild_source_credential" "cecw" {
  auth_type   = "PERSONAL_ACCESS_TOKEN"
  server_type = "GITHUB"
  token       = "cecw-github-token"
}

resource "aws_codebuild_project" "cecw" {
  name         = "cecw-project"
  service_role = "arn:aws:iam::000000000000:role/codebuild-role"

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:1.0"
    type         = "LINUX_CONTAINER"
  }

  source {
    type     = "GITHUB"
    location = "https://github.com/cecw/example.git"
  }
}

resource "aws_codebuild_webhook" "cecw" {
  project_name    = aws_codebuild_project.cecw.name
  build_type      = "BUILD"
  manual_creation = true
}

##############################################################################
# WAFv2: IP set, regex pattern set, rule group referencing both, an API key,
# a web ACL associated with an ALB, and a logging configuration.
##############################################################################

resource "aws_wafv2_ip_set" "cecw" {
  name               = "cecw-ip-set"
  scope              = "REGIONAL"
  ip_address_version = "IPV4"
  addresses          = ["10.0.0.0/16", "192.168.0.0/24"]
}

resource "aws_wafv2_regex_pattern_set" "cecw" {
  name  = "cecw-regex-pattern-set"
  scope = "REGIONAL"

  regular_expression {
    regex_string = "cecw-.*"
  }
}

resource "aws_wafv2_rule_group" "cecw" {
  name     = "cecw-rule-group"
  scope    = "REGIONAL"
  capacity = 25

  rule {
    name     = "block-ip-set"
    priority = 0

    action {
      block {}
    }

    statement {
      ip_set_reference_statement {
        arn = aws_wafv2_ip_set.cecw.arn
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "cecw-block-ip-set"
      sampled_requests_enabled   = false
    }
  }

  rule {
    name     = "block-regex"
    priority = 1

    action {
      block {}
    }

    statement {
      regex_pattern_set_reference_statement {
        arn = aws_wafv2_regex_pattern_set.cecw.arn

        field_to_match {
          uri_path {}
        }

        text_transformation {
          priority = 0
          type     = "NONE"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "cecw-block-regex"
      sampled_requests_enabled   = false
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "cecw-rule-group"
    sampled_requests_enabled   = false
  }
}

resource "aws_wafv2_api_key" "cecw" {
  scope         = "REGIONAL"
  token_domains = ["cecw.example.com"]
}

resource "aws_wafv2_web_acl" "cecw" {
  name  = "cecw-web-acl"
  scope = "REGIONAL"

  default_action {
    allow {}
  }

  rule {
    name     = "use-rule-group"
    priority = 0

    override_action {
      none {}
    }

    statement {
      rule_group_reference_statement {
        arn = aws_wafv2_rule_group.cecw.arn
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "cecw-use-rule-group"
      sampled_requests_enabled   = false
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "cecw-web-acl"
    sampled_requests_enabled   = false
  }
}

resource "aws_vpc" "cecw" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "cecw-vpc"
  }
}

resource "aws_subnet" "cecw_a" {
  vpc_id     = aws_vpc.cecw.id
  cidr_block = "{{.SubnetCidrA}}"

  tags = {
    Name = "cecw-subnet-a"
  }
}

resource "aws_subnet" "cecw_b" {
  vpc_id     = aws_vpc.cecw.id
  cidr_block = "{{.SubnetCidrB}}"

  tags = {
    Name = "cecw-subnet-b"
  }
}

resource "aws_lb" "cecw" {
  name               = "cecw-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.cecw_a.id, aws_subnet.cecw_b.id]
}

resource "aws_wafv2_web_acl_association" "cecw" {
  resource_arn = aws_lb.cecw.arn
  web_acl_arn  = aws_wafv2_web_acl.cecw.arn
}

resource "aws_cloudwatch_log_group" "cecw_waf" {
  name = "aws-waf-logs-cecw"
}

resource "aws_wafv2_web_acl_logging_configuration" "cecw" {
  resource_arn            = aws_wafv2_web_acl.cecw.arn
  log_destination_configs = [aws_cloudwatch_log_group.cecw_waf.arn]
}
