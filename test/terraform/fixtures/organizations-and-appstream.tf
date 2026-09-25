##############################################################################
# OpsWorks: a stack plus every "canned" layer type without coverage, an
# instance assigned to one of them, and an RDS DB instance registration.
##############################################################################

resource "aws_opsworks_stack" "orap" {
  name                         = "orap-stack"
  region                       = "us-east-1"
  service_role_arn             = "arn:aws:iam::000000000000:role/orap-opsworks-service-role"
  default_instance_profile_arn = "arn:aws:iam::000000000000:instance-profile/orap-opsworks-instance-profile"
}

resource "aws_opsworks_ecs_cluster_layer" "orap" {
  stack_id        = aws_opsworks_stack.orap.id
  ecs_cluster_arn = "arn:aws:ecs:us-east-1:000000000000:cluster/orap-cluster"
}

resource "aws_opsworks_ganglia_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
  password = "orappw"
}

resource "aws_opsworks_haproxy_layer" "orap" {
  stack_id       = aws_opsworks_stack.orap.id
  stats_password = "orapstatspw"
}

resource "aws_opsworks_java_app_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_memcached_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_mysql_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_nodejs_app_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_php_app_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_rails_app_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_static_web_layer" "orap" {
  stack_id = aws_opsworks_stack.orap.id
}

resource "aws_opsworks_instance" "orap" {
  stack_id      = aws_opsworks_stack.orap.id
  layer_ids     = [aws_opsworks_rails_app_layer.orap.id]
  instance_type = "m5.large"
  state         = "stopped"
}

resource "aws_opsworks_rds_db_instance" "orap" {
  stack_id            = aws_opsworks_stack.orap.id
  rds_db_instance_arn = "arn:aws:rds:us-east-1:000000000000:db:orap-db"
  db_user             = "orapadmin"
  db_password         = "orapdbpw"
}

##############################################################################
# AppStream: a directory config, a fleet + stack association, an image
# builder, a user, and a user/stack association.
##############################################################################

resource "aws_appstream_stack" "orap" {
  name = "orap-stack"
}

resource "aws_appstream_directory_config" "orap" {
  directory_name                          = "orap.example.test"
  organizational_unit_distinguished_names = ["OU=orap,DC=orap,DC=example,DC=test"]

  service_account_credentials {
    account_name     = "orap-service-account"
    account_password = "OrapPassword1!"
  }
}

resource "aws_appstream_fleet" "orap" {
  name          = "orap-fleet"
  image_name    = "AppStream-WinServer2019-06-01-2023"
  instance_type = "stream.standard.medium"
  fleet_type    = "ON_DEMAND"

  compute_capacity {
    desired_instances = 1
  }
}

resource "aws_appstream_fleet_stack_association" "orap" {
  fleet_name = aws_appstream_fleet.orap.name
  stack_name = aws_appstream_stack.orap.name
}

resource "aws_appstream_image_builder" "orap" {
  name          = "orap-image-builder"
  image_name    = "AppStream-WinServer2019-06-01-2023"
  instance_type = "stream.standard.medium"
}

resource "aws_appstream_user" "orap" {
  user_name           = "orap-user@example.test"
  authentication_type = "USERPOOL"
  first_name          = "Mega"
  last_name           = "BatchFortyEight"
}

resource "aws_appstream_user_stack_association" "orap" {
  authentication_type = "USERPOOL"
  stack_name          = aws_appstream_stack.orap.name
  user_name           = aws_appstream_user.orap.user_name
}

##############################################################################
# Organizations: an organization, a member account, a delegated
# administrator, a policy + attachment, and a resource-based delegation
# policy.
##############################################################################

resource "aws_organizations_organization" "orap" {
  feature_set = "ALL"
}

resource "aws_organizations_account" "orap" {
  name  = "orap-account"
  email = "orap@example.test"

  depends_on = [aws_organizations_organization.orap]
}

resource "terraform_data" "orap_enable_service_access" {
  triggers_replace = {
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' organizations enable-aws-service-access --service-principal config.amazonaws.com"
  }

  depends_on = [aws_organizations_organization.orap]
}

resource "aws_organizations_delegated_administrator" "orap" {
  account_id        = aws_organizations_account.orap.id
  service_principal = "config.amazonaws.com"

  depends_on = [terraform_data.orap_enable_service_access]
}

resource "aws_organizations_policy" "orap" {
  name = "orap-policy"
  type = "SERVICE_CONTROL_POLICY"
  content = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "Orap"
      Effect   = "Allow"
      Action   = "*"
      Resource = "*"
    }]
  })

  depends_on = [aws_organizations_organization.orap]
}

resource "aws_organizations_policy_attachment" "orap" {
  policy_id = aws_organizations_policy.orap.id
  target_id = aws_organizations_account.orap.id
}

resource "aws_organizations_resource_policy" "orap" {
  content = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "OrapResourcePolicy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::999999999999:root" }
      Action    = "organizations:DescribeResourcePolicy"
      Resource  = "*"
    }]
  })

  depends_on = [aws_organizations_organization.orap]
}
