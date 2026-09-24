##############################################################################
# DMS: a replication subnet group, a certificate, an S3 source endpoint (via
# the generic endpoint resource's s3_settings block) and dedicated S3 target
# endpoint, an event subscription, a replication instance/task, and a
# serverless replication config.
##############################################################################

resource "aws_vpc" "mb40" {
  cidr_block = "10.204.0.0/16"

  tags = {
    Name = "mega-batch-40-vpc"
  }
}

resource "aws_subnet" "mb40_a" {
  vpc_id            = aws_vpc.mb40.id
  cidr_block        = "10.204.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "mega-batch-40-subnet-a"
  }
}

resource "aws_subnet" "mb40_b" {
  vpc_id            = aws_vpc.mb40.id
  cidr_block        = "10.204.2.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "mega-batch-40-subnet-b"
  }
}

resource "aws_dms_replication_subnet_group" "mb40" {
  replication_subnet_group_id          = "mega-batch-40-subnet-group"
  replication_subnet_group_description = "mega-batch-40 subnet group"
  subnet_ids                           = [aws_subnet.mb40_a.id, aws_subnet.mb40_b.id]
}

resource "aws_dms_certificate" "mb40" {
  certificate_id  = "mega-batch-40-cert"
  certificate_pem = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAS6gAwIBAgIUV2VaBnegaBanana==\n-----END CERTIFICATE-----\n"
}

resource "aws_iam_role" "mb40_s3" {
  name = "mega-batch-40-dms-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dms.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "mb40_source" {
  bucket = "mega-batch-40-dms-source-bucket"
}

resource "aws_s3_bucket" "mb40_target" {
  bucket = "mega-batch-40-dms-target-bucket"
}

# S3 as a migration source, via the generic endpoint resource's s3_settings
# block -- distinct from the dedicated aws_dms_s3_endpoint resource used
# below for the target side.
resource "aws_dms_endpoint" "mb40_source" {
  endpoint_id   = "mega-batch-40-source"
  endpoint_type = "source"
  engine_name   = "s3"

  s3_settings {
    bucket_name             = aws_s3_bucket.mb40_source.id
    service_access_role_arn = aws_iam_role.mb40_s3.arn
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

resource "aws_dms_s3_endpoint" "mb40_target" {
  endpoint_id             = "mega-batch-40-target-s3"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.mb40_target.id
  service_access_role_arn = aws_iam_role.mb40_s3.arn
}

resource "aws_sns_topic" "mb40" {
  name = "mega-batch-40-dms-events"
}

resource "aws_dms_event_subscription" "mb40" {
  name             = "mega-batch-40-event-subscription"
  sns_topic_arn    = aws_sns_topic.mb40.arn
  source_type      = "replication-instance"
  event_categories = ["creation", "failure"]
  enabled          = true
}

resource "aws_dms_replication_instance" "mb40" {
  replication_instance_id     = "mega-batch-40-instance"
  replication_instance_class  = "dms.t3.medium"
  allocated_storage           = 20
  publicly_accessible         = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.mb40.id
}

resource "aws_dms_replication_task" "mb40" {
  replication_task_id      = "mega-batch-40-task"
  migration_type           = "full-load"
  replication_instance_arn = aws_dms_replication_instance.mb40.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.mb40_source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.mb40_target.endpoint_arn

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

resource "aws_dms_replication_config" "mb40" {
  replication_config_identifier = "mega-batch-40-serverless"
  replication_type              = "full-load"
  source_endpoint_arn           = aws_dms_endpoint.mb40_source.endpoint_arn
  target_endpoint_arn           = aws_dms_s3_endpoint.mb40_target.endpoint_arn

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
    replication_subnet_group_id = aws_dms_replication_subnet_group.mb40.id
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

resource "aws_emr_block_public_access_configuration" "mb40" {
  block_public_security_group_rules = true

  permitted_public_security_group_rule_range {
    min_range = 22
    max_range = 22
  }
}

resource "aws_emr_security_configuration" "mb40" {
  name = "mega-batch-40-security-config"

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

resource "aws_emr_cluster" "mb40_groups" {
  name                   = "mega-batch-40-emr-groups"
  release_label          = "emr-6.0.0"
  service_role           = "arn:aws:iam::000000000000:role/emr-service-role"
  security_configuration = aws_emr_security_configuration.mb40.name
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

resource "aws_emr_instance_group" "mb40_task" {
  cluster_id     = aws_emr_cluster.mb40_groups.id
  instance_type  = "m4.large"
  instance_count = 1
  name           = "mega-batch-40-task-group"
}

resource "aws_emr_managed_scaling_policy" "mb40" {
  cluster_id = aws_emr_cluster.mb40_groups.id

  compute_limits {
    unit_type              = "Instances"
    minimum_capacity_units = 1
    maximum_capacity_units = 4
  }
}

resource "aws_emr_cluster" "mb40_fleets" {
  name          = "mega-batch-40-emr-fleets"
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

resource "aws_emr_instance_fleet" "mb40_task" {
  cluster_id = aws_emr_cluster.mb40_fleets.id
  name       = "mega-batch-40-task-fleet"

  instance_type_configs {
    instance_type = "m4.large"
  }

  target_on_demand_capacity = 1
}

resource "aws_iam_role" "mb40_studio" {
  name = "mega-batch-40-studio-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_security_group" "mb40_studio_engine" {
  name   = "mega-batch-40-studio-engine-sg"
  vpc_id = aws_vpc.mb40.id
}

resource "aws_security_group" "mb40_studio_workspace" {
  name   = "mega-batch-40-studio-workspace-sg"
  vpc_id = aws_vpc.mb40.id
}

resource "aws_s3_bucket" "mb40_studio" {
  bucket = "mega-batch-40-studio-bucket"
}

resource "aws_emr_studio" "mb40" {
  name                        = "mega-batch-40-studio"
  auth_mode                   = "IAM"
  default_s3_location         = "s3://${aws_s3_bucket.mb40_studio.id}/studio"
  engine_security_group_id    = aws_security_group.mb40_studio_engine.id
  workspace_security_group_id = aws_security_group.mb40_studio_workspace.id
  service_role                = aws_iam_role.mb40_studio.arn
  subnet_ids                  = [aws_subnet.mb40_a.id, aws_subnet.mb40_b.id]
  vpc_id                      = aws_vpc.mb40.id
}

resource "aws_iam_policy" "mb40_session" {
  name = "mega-batch-40-session-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "elasticmapreduce:*"
      Resource = "*"
    }]
  })
}

resource "aws_emr_studio_session_mapping" "mb40" {
  studio_id          = aws_emr_studio.mb40.id
  identity_type      = "USER"
  identity_name      = "mega-batch-40-user"
  session_policy_arn = aws_iam_policy.mb40_session.arn
}
