##############################################################################
# DMS: a replication subnet group, a certificate, an S3 source endpoint (via
# the generic endpoint resource's s3_settings block) and dedicated S3 target
# endpoint, an event subscription, a replication instance/task, and a
# serverless replication config.
##############################################################################

resource "aws_vpc" "dmse" {
  cidr_block = "10.204.0.0/16"

  tags = {
    Name = "dmse-vpc"
  }
}

resource "aws_subnet" "dmse_a" {
  vpc_id            = aws_vpc.dmse.id
  cidr_block        = "10.204.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "dmse-subnet-a"
  }
}

resource "aws_subnet" "dmse_b" {
  vpc_id            = aws_vpc.dmse.id
  cidr_block        = "10.204.2.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "dmse-subnet-b"
  }
}

resource "aws_dms_replication_subnet_group" "dmse" {
  replication_subnet_group_id          = "dmse-subnet-group"
  replication_subnet_group_description = "dmse subnet group"
  subnet_ids                           = [aws_subnet.dmse_a.id, aws_subnet.dmse_b.id]
}

resource "aws_dms_certificate" "dmse" {
  certificate_id  = "dmse-cert"
  certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAS6gAwIBAgIUV2VaBnegaBanana==\n-----END CERTIFICATE-----\n"
}

resource "aws_iam_role" "dmse_s3" {
  name = "dmse-dms-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dms.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "dmse_source" {
  bucket = "dmse-dms-source-bucket"
}

resource "aws_s3_bucket" "dmse_target" {
  bucket = "dmse-dms-target-bucket"
}

# S3 as a migration source, via the generic endpoint resource's s3_settings
# block -- distinct from the dedicated aws_dms_s3_endpoint resource used
# below for the target side.
resource "aws_dms_endpoint" "dmse_source" {
  endpoint_id   = "dmse-source"
  endpoint_type = "source"
  engine_name   = "s3"

  s3_settings {
    bucket_name             = aws_s3_bucket.dmse_source.id
    service_access_role_arn = aws_iam_role.dmse_s3.arn
    external_table_definition = jsonencode({
      TableCount = "1"
      Tables = [{
        TableName    = "example"
        TablePath    = "example/"
        TableColumns = [{ ColumnName = "id", ColumnType = "STRING", ColumnLength = "20" }]
      }]
    })
  }
}

resource "aws_dms_s3_endpoint" "dmse_target" {
  endpoint_id             = "dmse-target-s3"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.dmse_target.id
  service_access_role_arn = aws_iam_role.dmse_s3.arn
}

resource "aws_sns_topic" "dmse" {
  name = "dmse-dms-events"
}

resource "aws_dms_event_subscription" "dmse" {
  name             = "dmse-event-subscription"
  sns_topic_arn    = aws_sns_topic.dmse.arn
  source_type      = "replication-instance"
  event_categories = ["creation", "failure"]
  enabled          = true
}

resource "aws_dms_replication_instance" "dmse" {
  replication_instance_id     = "dmse-instance"
  replication_instance_class  = "dms.t3.medium"
  allocated_storage           = 20
  publicly_accessible         = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.dmse.id
}

resource "aws_dms_replication_task" "dmse" {
  replication_task_id      = "dmse-task"
  migration_type           = "full-load"
  replication_instance_arn = aws_dms_replication_instance.dmse.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.dmse_source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.dmse_target.endpoint_arn

  table_mappings = jsonencode({
    rules = [{
      rule-type = "selection"
      rule-id   = "1"
      rule-name = "1"
      object-locator = {
        schema-name = "%"
        table-name  = "%"
      }
      rule-action = "include"
    }]
  })
}

resource "aws_dms_replication_config" "dmse" {
  replication_config_identifier = "dmse-serverless"
  replication_type              = "full-load"
  source_endpoint_arn           = aws_dms_endpoint.dmse_source.endpoint_arn
  target_endpoint_arn           = aws_dms_s3_endpoint.dmse_target.endpoint_arn

  table_mappings = jsonencode({
    rules = [{
      rule-type = "selection"
      rule-id   = "1"
      rule-name = "1"
      object-locator = {
        schema-name = "%"
        table-name  = "%"
      }
      rule-action = "include"
    }]
  })

  compute_config {
    replication_subnet_group_id = aws_dms_replication_subnet_group.dmse.id
    max_capacity_units          = 4
    min_capacity_units          = 1
  }
}

##############################################################################
# EMR: an account-level block public access configuration, a security
# configuration, an instance-group cluster with a task instance group and a
# managed scaling policy, an instance-fleet cluster with a task instance
# fleet, and a studio with a user session mapping.
##############################################################################

resource "aws_emr_block_public_access_configuration" "dmse" {
  block_public_security_group_rules = true

  permitted_public_security_group_rule_range {
    min_range = 22
    max_range = 22
  }
}

resource "aws_emr_security_configuration" "dmse" {
  name = "dmse-security-config"

  configuration = jsonencode({
    EncryptionConfiguration = {
      EnableInTransitEncryption = false
      EnableAtRestEncryption    = true
      AtRestEncryptionConfiguration = {
        S3EncryptionConfiguration = {
          EncryptionMode = "SSE-S3"
        }
      }
    }
  })
}

resource "aws_emr_cluster" "dmse_groups" {
  name                   = "dmse-emr-groups"
  release_label          = "emr-6.0.0"
  service_role           = "arn:aws:iam::000000000000:role/emr-service-role"
  security_configuration = aws_emr_security_configuration.dmse.name
  applications           = ["Hadoop"]

  ec2_attributes {
    instance_profile = "arn:aws:iam::000000000000:instance-profile/emr-ec2-role"
  }

  master_instance_group {
    instance_type = "m4.large"
  }

  core_instance_group {
    instance_type  = "m4.large"
    instance_count = 1
  }
}

resource "aws_emr_instance_group" "dmse_task" {
  cluster_id     = aws_emr_cluster.dmse_groups.id
  instance_type  = "m4.large"
  instance_count = 1
  name           = "dmse-task-group"
}

resource "aws_emr_managed_scaling_policy" "dmse" {
  cluster_id = aws_emr_cluster.dmse_groups.id

  compute_limits {
    unit_type              = "Instances"
    minimum_capacity_units = 1
    maximum_capacity_units = 4
  }
}

resource "aws_emr_cluster" "dmse_fleets" {
  name          = "dmse-emr-fleets"
  release_label = "emr-6.0.0"
  service_role  = "arn:aws:iam::000000000000:role/emr-service-role"
  applications  = ["Hadoop"]

  ec2_attributes {
    instance_profile = "arn:aws:iam::000000000000:instance-profile/emr-ec2-role"
  }

  master_instance_fleet {
    instance_type_configs {
      instance_type = "m4.large"
    }
    target_on_demand_capacity = 1
  }

  core_instance_fleet {
    instance_type_configs {
      instance_type = "m4.large"
    }
    target_on_demand_capacity = 1
  }
}

resource "aws_emr_instance_fleet" "dmse_task" {
  cluster_id = aws_emr_cluster.dmse_fleets.id
  name       = "dmse-task-fleet"

  instance_type_configs {
    instance_type = "m4.large"
  }

  target_on_demand_capacity = 1
}

resource "aws_iam_role" "dmse_studio" {
  name = "dmse-studio-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_security_group" "dmse_studio_engine" {
  name   = "dmse-studio-engine-sg"
  vpc_id = aws_vpc.dmse.id
}

resource "aws_security_group" "dmse_studio_workspace" {
  name   = "dmse-studio-workspace-sg"
  vpc_id = aws_vpc.dmse.id
}

resource "aws_s3_bucket" "dmse_studio" {
  bucket = "dmse-studio-bucket"
}

resource "aws_emr_studio" "dmse" {
  name                        = "dmse-studio"
  auth_mode                   = "IAM"
  default_s3_location         = "s3://${aws_s3_bucket.dmse_studio.id}/studio"
  engine_security_group_id    = aws_security_group.dmse_studio_engine.id
  workspace_security_group_id = aws_security_group.dmse_studio_workspace.id
  service_role                = aws_iam_role.dmse_studio.arn
  subnet_ids                  = [aws_subnet.dmse_a.id, aws_subnet.dmse_b.id]
  vpc_id                      = aws_vpc.dmse.id
}

resource "aws_iam_policy" "dmse_session" {
  name = "dmse-session-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "elasticmapreduce:*"
      Resource = "*"
    }]
  })
}

resource "aws_emr_studio_session_mapping" "dmse" {
  studio_id          = aws_emr_studio.dmse.id
  identity_type      = "USER"
  identity_name      = "dmse-user"
  session_policy_arn = aws_iam_policy.dmse_session.arn
}
