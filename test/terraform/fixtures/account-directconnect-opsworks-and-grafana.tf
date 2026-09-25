resource "aws_account_primary_contact" "example" {
  full_name       = "Mega Batch Nine"
  address_line_1  = "1200 12th Ave S"
  city            = "Seattle"
  state_or_region = "WA"
  postal_code     = "98144"
  country_code    = "US"
  phone_number    = "+12065550100"
}

resource "aws_account_region" "example" {
  region_name = "af-south-1"
  enabled     = true
}

resource "aws_dx_gateway" "example" {
  name            = "mega-batch-9-dxgw"
  amazon_side_asn = 64512
}

resource "aws_vpn_gateway" "example" {
  tags = {
    Name = "mega-batch-9-vgw"
  }
}

resource "aws_dx_gateway_association" "example" {
  dx_gateway_id         = aws_dx_gateway.example.id
  associated_gateway_id = aws_vpn_gateway.example.id
}

resource "aws_dx_lag" "example" {
  name                  = "mega-batch-9-lag"
  connections_bandwidth = "1Gbps"
  location              = "EqDC2"
}

resource "aws_opsworks_stack" "example" {
  name                         = "mega-batch-9-stack"
  region                       = "us-east-1"
  service_role_arn             = "arn:aws:iam::000000000000:role/mega-batch-9-opsworks-service-role"
  default_instance_profile_arn = "arn:aws:iam::000000000000:instance-profile/mega-batch-9-opsworks-instance-profile"
}

resource "aws_opsworks_custom_layer" "example" {
  name       = "mega-batch-9-layer"
  short_name = "mb9layer"
  stack_id   = aws_opsworks_stack.example.id
}

resource "aws_opsworks_application" "example" {
  name     = "mega-batch-9-app"
  stack_id = aws_opsworks_stack.example.id
  type     = "other"
}

resource "aws_opsworks_user_profile" "example" {
  user_arn     = "arn:aws:iam::000000000000:user/mega-batch-9-user"
  ssh_username = "mega-batch-9-user"
}

resource "aws_opsworks_permission" "example" {
  stack_id = aws_opsworks_stack.example.id
  user_arn = aws_opsworks_user_profile.example.user_arn
  level    = "deploy"
}

resource "aws_grafana_workspace" "example" {
  name                     = "mega-batch-9-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
}

resource "aws_grafana_workspace_api_key" "example" {
  key_name        = "mega-batch-9-key"
  key_role        = "ADMIN"
  seconds_to_live = 3600
  workspace_id    = aws_grafana_workspace.example.id
}

data "aws_ssoadmin_instances" "example" {}

resource "aws_identitystore_user" "example" {
  identity_store_id = tolist(data.aws_ssoadmin_instances.example.identity_store_ids)[0]
  display_name      = "Mega Batch Nine"
  user_name         = "mega-batch-9-user"

  name {
    given_name  = "Mega"
    family_name = "Batch"
  }
}

resource "aws_grafana_role_association" "example" {
  role         = "ADMIN"
  workspace_id = aws_grafana_workspace.example.id
  user_ids     = [aws_identitystore_user.example.user_id]
}
