##############################################################################
# Cognito IDP: identity provider, resource server, user pool domain, UI
# customization, and risk configuration on a shared user pool.
##############################################################################

resource "aws_cognito_user_pool" "mb37" {
  name = "mega-batch-37-pool"
}

resource "aws_cognito_user_pool_client" "mb37" {
  name         = "mega-batch-37-client"
  user_pool_id = aws_cognito_user_pool.mb37.id
}

resource "aws_cognito_identity_provider" "mb37" {
  user_pool_id  = aws_cognito_user_pool.mb37.id
  provider_name = "MegaBatch37Google"
  provider_type = "Google"

  provider_details = {
    client_id        = "mega-batch-37-client-id"
    client_secret    = "mega-batch-37-client-secret"
    authorize_scopes = "email profile"
  }

  attribute_mapping = {
    email = "email"
  }
}

resource "aws_cognito_resource_server" "mb37" {
  user_pool_id = aws_cognito_user_pool.mb37.id
  identifier   = "mega-batch-37-api"
  name         = "mega-batch-37-resource-server"

  scope {
    scope_name        = "read"
    scope_description = "read access"
  }

  scope {
    scope_name        = "write"
    scope_description = "write access"
  }
}

resource "aws_cognito_user_pool_domain" "mb37" {
  domain       = "mega-batch-37-domain"
  user_pool_id = aws_cognito_user_pool.mb37.id
}

resource "aws_cognito_user_pool_ui_customization" "mb37" {
  user_pool_id = aws_cognito_user_pool.mb37.id
  client_id    = "ALL"
  css          = ".label-customizable {font-weight: 400;}"

  depends_on = [aws_cognito_user_pool_domain.mb37]
}

resource "aws_cognito_risk_configuration" "mb37" {
  user_pool_id = aws_cognito_user_pool.mb37.id

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

resource "aws_ecs_account_setting_default" "mb37" {
  name  = "containerInsights"
  value = "enabled"
}

resource "aws_launch_template" "mb37_ecs" {
  name_prefix   = "mega-batch-37-lt-"
  image_id      = "ami-0c55b159cbfafe1f0"
  instance_type = "t2.micro"
}

resource "aws_autoscaling_group" "mb37_ecs" {
  name                = "mega-batch-37-ecs-asg"
  min_size            = 1
  max_size            = 3
  desired_capacity    = 1
  availability_zones  = ["us-east-1a"]

  launch_template {
    id      = aws_launch_template.mb37_ecs.id
    version = "$Latest"
  }
}

resource "aws_ecs_capacity_provider" "mb37" {
  name = "mega-batch-37-cp"

  auto_scaling_group_provider {
    auto_scaling_group_arn = aws_autoscaling_group.mb37_ecs.arn

    managed_scaling {
      status          = "ENABLED"
      target_capacity = 75
    }
  }
}

resource "aws_ecs_cluster" "mb37" {
  name = "mega-batch-37-cluster"
}

resource "aws_ecs_cluster_capacity_providers" "mb37" {
  cluster_name       = aws_ecs_cluster.mb37.name
  capacity_providers = [aws_ecs_capacity_provider.mb37.name]

  default_capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.mb37.name
    weight            = 1
  }
}

resource "aws_ecs_tag" "mb37" {
  resource_arn = aws_ecs_cluster.mb37.arn
  key          = "mega-batch-37-key"
  value        = "mega-batch-37-value"
}

resource "aws_ecs_task_definition" "mb37" {
  family = "mega-batch-37-task"

  container_definitions = jsonencode([
    {
      name      = "nginx"
      image     = "nginx:latest"
      essential = true
    }
  ])
}

resource "aws_ecs_service" "mb37" {
  name                  = "mega-batch-37-service"
  cluster               = aws_ecs_cluster.mb37.arn
  desired_count         = 1
  wait_for_steady_state = false

  deployment_controller {
    type = "EXTERNAL"
  }
}

resource "aws_ecs_task_set" "mb37" {
  cluster         = aws_ecs_cluster.mb37.arn
  service         = aws_ecs_service.mb37.id
  task_definition = aws_ecs_task_definition.mb37.arn
}

##############################################################################
# CloudFormation: self-managed StackSet with a singular stack-set instance
# and a bulk stack-instances deployment target, plus a private resource type
# registration.
##############################################################################

resource "aws_iam_role" "mb37_cfn_admin" {
  name = "mega-batch-37-cfn-admin-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudformation.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudformation_stack_set" "mb37" {
  name                    = "mega-batch-37-stackset"
  administration_role_arn = aws_iam_role.mb37_cfn_admin.arn
  execution_role_name     = "mega-batch-37-cfn-exec-role"

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

resource "aws_cloudformation_stack_set_instance" "mb37" {
  stack_set_name = aws_cloudformation_stack_set.mb37.name
  account_id     = "000000000000"
  region         = "us-east-1"
}

# Separate stack set: the provider reads back every instance of a stack set
# as the resource's accounts/regions, so sharing one with the singular
# stack_set_instance above would never re-plan empty.
resource "aws_cloudformation_stack_set" "mb37_bulk" {
  name                    = "mega-batch-37-stackset-bulk"
  administration_role_arn = aws_iam_role.mb37_cfn_admin.arn
  execution_role_name     = "mega-batch-37-cfn-exec-role"
  template_body           = aws_cloudformation_stack_set.mb37.template_body
}

resource "aws_cloudformation_stack_instances" "mb37" {
  stack_set_name = aws_cloudformation_stack_set.mb37_bulk.name
  regions        = ["us-west-2"]

  # Without deployment_targets the provider replaces `accounts` with its own
  # caller account id, which is "" under skip_requesting_account_id.
  deployment_targets {
    accounts = ["000000000000"]
  }
}

resource "aws_cloudformation_type" "mb37" {
  type                  = "RESOURCE"
  type_name             = "MegaBatch37::Example::Resource"
  schema_handler_package = "s3://mega-batch-37-bucket/schema-handler.zip"
}

##############################################################################
# CodeBuild: compute fleet, report group with a resource policy, a source
# credential, and a GitHub-sourced project with a webhook.
##############################################################################

resource "aws_codebuild_fleet" "mb37" {
  name             = "mega-batch-37-fleet"
  base_capacity    = 1
  compute_type     = "BUILD_GENERAL1_SMALL"
  environment_type = "LINUX_CONTAINER"
}

resource "aws_codebuild_report_group" "mb37" {
  name = "mega-batch-37-report-group"
  type = "TEST"

  export_config {
    type = "NO_EXPORT"
  }
}

resource "aws_codebuild_resource_policy" "mb37" {
  resource_arn = aws_codebuild_report_group.mb37.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mega-batch-37-policy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "codebuild:BatchGetReportGroups"
      Resource  = aws_codebuild_report_group.mb37.arn
    }]
  })
}

resource "aws_codebuild_source_credential" "mb37" {
  auth_type   = "PERSONAL_ACCESS_TOKEN"
  server_type = "GITHUB"
  token       = "mega-batch-37-github-token"
}

resource "aws_codebuild_project" "mb37" {
  name         = "mega-batch-37-project"
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
    location = "https://github.com/mega-batch-37/example.git"
  }
}

resource "aws_codebuild_webhook" "mb37" {
  project_name    = aws_codebuild_project.mb37.name
  build_type      = "BUILD"
  manual_creation = true
}

##############################################################################
# WAFv2: IP set, regex pattern set, rule group referencing both, an API key,
# a web ACL associated with an ALB, and a logging configuration.
##############################################################################

resource "aws_wafv2_ip_set" "mb37" {
  name               = "mega-batch-37-ip-set"
  scope              = "REGIONAL"
  ip_address_version = "IPV4"
  addresses          = ["10.0.0.0/16", "192.168.0.0/24"]
}

resource "aws_wafv2_regex_pattern_set" "mb37" {
  name  = "mega-batch-37-regex-pattern-set"
  scope = "REGIONAL"

  regular_expression {
    regex_string = "mega-batch-37-.*"
  }
}

resource "aws_wafv2_rule_group" "mb37" {
  name     = "mega-batch-37-rule-group"
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
        arn = aws_wafv2_ip_set.mb37.arn
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "mega-batch-37-block-ip-set"
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
        arn = aws_wafv2_regex_pattern_set.mb37.arn

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
      metric_name                = "mega-batch-37-block-regex"
      sampled_requests_enabled   = false
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "mega-batch-37-rule-group"
    sampled_requests_enabled   = false
  }
}

resource "aws_wafv2_api_key" "mb37" {
  scope         = "REGIONAL"
  token_domains = ["mega-batch-37.example.com"]
}

resource "aws_wafv2_web_acl" "mb37" {
  name  = "mega-batch-37-web-acl"
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
        arn = aws_wafv2_rule_group.mb37.arn
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "mega-batch-37-use-rule-group"
      sampled_requests_enabled   = false
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "mega-batch-37-web-acl"
    sampled_requests_enabled   = false
  }
}

resource "aws_vpc" "mb37" {
  cidr_block = "{{.VPCCidr}}"

  tags = {
    Name = "mega-batch-37-vpc"
  }
}

resource "aws_subnet" "mb37_a" {
  vpc_id     = aws_vpc.mb37.id
  cidr_block = "{{.SubnetCidrA}}"

  tags = {
    Name = "mega-batch-37-subnet-a"
  }
}

resource "aws_subnet" "mb37_b" {
  vpc_id     = aws_vpc.mb37.id
  cidr_block = "{{.SubnetCidrB}}"

  tags = {
    Name = "mega-batch-37-subnet-b"
  }
}

resource "aws_lb" "mb37" {
  name               = "mega-batch-37-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.mb37_a.id, aws_subnet.mb37_b.id]
}

resource "aws_wafv2_web_acl_association" "mb37" {
  resource_arn = aws_lb.mb37.arn
  web_acl_arn  = aws_wafv2_web_acl.mb37.arn
}

resource "aws_cloudwatch_log_group" "mb37_waf" {
  name = "aws-waf-logs-mega-batch-37"
}

resource "aws_wafv2_web_acl_logging_configuration" "mb37" {
  resource_arn            = aws_wafv2_web_acl.mb37.arn
  log_destination_configs = [aws_cloudwatch_log_group.mb37_waf.arn]
}
