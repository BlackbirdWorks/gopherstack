# --- DataSync ---------------------------------------------------------------

resource "aws_datasync_agent" "example" {
  name           = "dssv-agent"
  activation_key = "DSSV-ACTIVATION-KEY"
}

resource "aws_vpc" "ds" {
  cidr_block = "10.151.0.0/16"

  tags = {
    Name = "dssv-vpc"
  }
}

resource "aws_subnet" "ds_a" {
  vpc_id     = aws_vpc.ds.id
  cidr_block = "10.151.1.0/24"

  tags = {
    Name = "dssv-subnet-a"
  }
}

resource "aws_security_group" "ds" {
  name   = "dssv-sg"
  vpc_id = aws_vpc.ds.id
}

resource "aws_efs_file_system" "example" {
  tags = {
    Name = "dssv-efs"
  }
}

resource "aws_efs_mount_target" "example" {
  file_system_id  = aws_efs_file_system.example.id
  subnet_id       = aws_subnet.ds_a.id
  security_groups = [aws_security_group.ds.id]
}

resource "aws_datasync_location_efs" "example" {
  efs_file_system_arn = aws_efs_mount_target.example.file_system_arn

  ec2_config {
    security_group_arns = [aws_security_group.ds.arn]
    subnet_arn          = aws_subnet.ds_a.arn
  }
}

resource "aws_datasync_location_nfs" "example" {
  server_hostname = "nfs.dssv.example.com"
  subdirectory    = "/exported/path"

  on_prem_config {
    agent_arns = [aws_datasync_agent.example.arn]
  }
}

resource "aws_datasync_location_smb" "example" {
  agent_arns      = [aws_datasync_agent.example.arn]
  server_hostname = "smb.dssv.example.com"
  subdirectory    = "/exported/path"
  user            = "Guest"
  password        = "ANotGreatPassword"
}

resource "aws_datasync_location_hdfs" "example" {
  agent_arns          = [aws_datasync_agent.example.arn]
  authentication_type = "SIMPLE"
  simple_user         = "dssv-user"

  name_node {
    hostname = "namenode.dssv.example.com"
    port     = 80
  }
}

resource "aws_datasync_location_object_storage" "example" {
  agent_arns      = [aws_datasync_agent.example.arn]
  server_hostname = "objectstore.dssv.example.com"
  bucket_name     = "dssv-bucket"
}

resource "aws_datasync_location_azure_blob" "example" {
  agent_arns          = [aws_datasync_agent.example.arn]
  authentication_type = "SAS"
  container_url       = "https://dssv.blob.core.windows.net/dssv-container"

  sas_configuration {
    token = "sp=r&st=2023-12-20T14:54:52Z&se=2023-12-20T22:54:52Z&spr=https&sv=2021-06-08&sr=c&sig=aBBKDWQvyuVcTPH9EBp%2FXTI9E%2F%2Fmq171%2BZU178wcwqU%3D"
  }
}

resource "aws_datasync_task" "example" {
  name                     = "dssv-task"
  source_location_arn      = aws_datasync_location_nfs.example.arn
  destination_location_arn = aws_datasync_location_efs.example.arn

  options {
    bytes_per_second = -1
  }
}

# --- SESv2 -------------------------------------------------------------------

resource "aws_sesv2_email_identity" "example" {
  email_identity = "dssv.example.com"
}

resource "aws_sesv2_email_identity_feedback_attributes" "example" {
  email_identity           = aws_sesv2_email_identity.example.email_identity
  email_forwarding_enabled = true
}

resource "aws_sesv2_email_identity_mail_from_attributes" "example" {
  email_identity = aws_sesv2_email_identity.example.email_identity

  behavior_on_mx_failure = "USE_DEFAULT_VALUE"
  mail_from_domain       = "bounce.dssv.example.com"
}

resource "aws_sesv2_email_identity_policy" "example" {
  email_identity = aws_sesv2_email_identity.example.email_identity
  policy_name    = "dssv-identity-policy"

  policy = jsonencode({
    Id      = "dssv-policy"
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AuthorizeSend"
      Effect    = "Allow"
      Resource  = "arn:aws:ses:us-east-1:000000000000:identity/dssv.example.com"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = ["ses:SendEmail"]
    }]
  })
}

resource "aws_sesv2_configuration_set" "example" {
  configuration_set_name = "dssv-config-set"
}

resource "aws_sns_topic" "sesv2" {
  name = "dssv-sesv2-topic"
}

resource "aws_sesv2_configuration_set_event_destination" "example" {
  configuration_set_name = aws_sesv2_configuration_set.example.configuration_set_name
  event_destination_name = "dssv-event-dest"

  event_destination {
    enabled              = true
    matching_event_types = ["SEND"]

    sns_destination {
      topic_arn = aws_sns_topic.sesv2.arn
    }
  }
}

resource "aws_sesv2_contact_list" "example" {
  contact_list_name = "dssv-contacts"
  description       = "dssv contact list"

  topic {
    default_subscription_status = "OPT_IN"
    description                 = "dssv topic"
    display_name                = "Dssv Topic"
    topic_name                  = "dssv-topic"
  }
}

resource "aws_sesv2_dedicated_ip_pool" "example" {
  pool_name = "dssv-pool"
}

resource "aws_sesv2_dedicated_ip_assignment" "example" {
  ip                    = "10.20.30.40"
  destination_pool_name = aws_sesv2_dedicated_ip_pool.example.pool_name
}

resource "aws_sesv2_account_suppression_attributes" "example" {
  suppressed_reasons = ["BOUNCE", "COMPLAINT"]
}

resource "aws_sesv2_account_vdm_attributes" "example" {
  vdm_enabled = "ENABLED"

  dashboard_attributes {
    engagement_metrics = "ENABLED"
  }

  guardian_attributes {
    optimized_shared_delivery = "ENABLED"
  }
}
