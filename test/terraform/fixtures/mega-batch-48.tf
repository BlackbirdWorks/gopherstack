##############################################################################
# OpsWorks: a stack plus every "canned" layer type without coverage, an
# instance assigned to one of them, and an RDS DB instance registration.
##############################################################################

resource "aws_opsworks_stack" "mb48" {
  name                         = "mega-batch-48-stack"
  region                       = "us-east-1"
  service_role_arn             = "arn:aws:iam::000000000000:role/mega-batch-48-opsworks-service-role"
  default_instance_profile_arn = "arn:aws:iam::000000000000:instance-profile/mega-batch-48-opsworks-instance-profile"
}

resource "aws_opsworks_ecs_cluster_layer" "mb48" {
  stack_id        = aws_opsworks_stack.mb48.id
  ecs_cluster_arn = "arn:aws:ecs:us-east-1:000000000000:cluster/mega-batch-48-cluster"
}

resource "aws_opsworks_ganglia_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
  password = "megabatch48pw"
}

resource "aws_opsworks_haproxy_layer" "mb48" {
  stack_id       = aws_opsworks_stack.mb48.id
  stats_password = "megabatch48statspw"
}

resource "aws_opsworks_java_app_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_memcached_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_mysql_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_nodejs_app_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_php_app_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_rails_app_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_static_web_layer" "mb48" {
  stack_id = aws_opsworks_stack.mb48.id
}

resource "aws_opsworks_instance" "mb48" {
  stack_id      = aws_opsworks_stack.mb48.id
  layer_ids     = [aws_opsworks_rails_app_layer.mb48.id]
  instance_type = "m5.large"
  state         = "stopped"
}

resource "aws_opsworks_rds_db_instance" "mb48" {
  stack_id            = aws_opsworks_stack.mb48.id
  rds_db_instance_arn = "arn:aws:rds:us-east-1:000000000000:db:mega-batch-48-db"
  db_user             = "mb48admin"
  db_password         = "megabatch48dbpw"
}

##############################################################################
# AppStream: a directory config, a fleet + stack association, an image
# builder, a user, and a user/stack association.
##############################################################################

resource "aws_appstream_stack" "mb48" {
  name = "mega-batch-48-stack"
}

resource "aws_appstream_directory_config" "mb48" {
  directory_name                          = "mega-batch-48.example.test"
  organizational_unit_distinguished_names = ["OU=mb48,DC=mega-batch-48,DC=example,DC=test"]

  service_account_credentials {
    account_name     = "mb48-service-account"
    account_password = "MegaBatch48Password1!"
  }
}

resource "aws_appstream_fleet" "mb48" {
  name          = "mega-batch-48-fleet"
  image_name    = "AppStream-WinServer2019-06-01-2023"
  instance_type = "stream.standard.medium"
  fleet_type    = "ON_DEMAND"

  compute_capacity {
    desired_instances = 1
  }
}

resource "aws_appstream_fleet_stack_association" "mb48" {
  fleet_name = aws_appstream_fleet.mb48.name
  stack_name = aws_appstream_stack.mb48.name
}

resource "aws_appstream_image_builder" "mb48" {
  name          = "mega-batch-48-image-builder"
  image_name    = "AppStream-WinServer2019-06-01-2023"
  instance_type = "stream.standard.medium"
}

resource "aws_appstream_user" "mb48" {
  user_name           = "mega-batch-48-user@example.test"
  authentication_type = "USERPOOL"
  first_name          = "Mega"
  last_name           = "BatchFortyEight"
}

resource "aws_appstream_user_stack_association" "mb48" {
  authentication_type = "USERPOOL"
  stack_name          = aws_appstream_stack.mb48.name
  user_name           = aws_appstream_user.mb48.user_name
}

##############################################################################
# Organizations: an organization, a member account, a delegated
# administrator, a policy + attachment, and a resource-based delegation
# policy.
##############################################################################

resource "aws_organizations_organization" "mb48" {
  feature_set = "ALL"
}

resource "aws_organizations_account" "mb48" {
  name  = "mega-batch-48-account"
  email = "mega-batch-48@example.test"

  depends_on = [aws_organizations_organization.mb48]
}

resource "terraform_data" "mb48_enable_service_access" {
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

  depends_on = [aws_organizations_organization.mb48]
}

resource "aws_organizations_delegated_administrator" "mb48" {
  account_id        = aws_organizations_account.mb48.id
  service_principal = "config.amazonaws.com"

  depends_on = [terraform_data.mb48_enable_service_access]
}

resource "aws_organizations_policy" "mb48" {
  name = "mega-batch-48-policy"
  type = "SERVICE_CONTROL_POLICY"
  content = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "MegaBatch48"
      Effect   = "Allow"
      Action   = "*"
      Resource = "*"
    }]
  })

  depends_on = [aws_organizations_organization.mb48]
}

resource "aws_organizations_policy_attachment" "mb48" {
  policy_id = aws_organizations_policy.mb48.id
  target_id = aws_organizations_account.mb48.id
}

resource "aws_organizations_resource_policy" "mb48" {
  content = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch48ResourcePolicy"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::999999999999:root" }
      Action    = "organizations:DescribeResourcePolicy"
      Resource  = "*"
    }]
  })

  depends_on = [aws_organizations_organization.mb48]
}
