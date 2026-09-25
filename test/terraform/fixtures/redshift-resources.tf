# --- IAM / KMS / S3 / SNS ----------------------------------------------------

resource "aws_iam_role" "redshift" {
  name = "rdsh-redshift-role-{{.Suffix}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["redshift.amazonaws.com", "scheduler.redshift.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kms_key" "this" {
  description = "rdsh kms key"
}

resource "aws_s3_bucket" "logs" {
  bucket = "rdsh-logs-bucket-{{.Suffix}}"
}

resource "aws_sns_topic" "events" {
  name = "rdsh-events-{{.Suffix}}"
}

# --- Redshift subnet group / parameter group / cluster -----------------------
# Subnet/security-group IDs are literal placeholders (as in the docdb/neptune
# and rds-resources fixtures) rather than real aws_vpc/aws_subnet/aws_security_
# group resources: Redshift's subnet group backend never cross-checks against
# EC2 state.

resource "aws_redshift_subnet_group" "this" {
  name       = "rdsh-subnet-group-{{.Suffix}}"
  subnet_ids = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_redshift_parameter_group" "this" {
  name   = "rdsh-params-{{.Suffix}}"
  family = "redshift-1.0"

  parameter {
    name  = "require_ssl"
    value = "true"
  }
}

resource "aws_redshift_cluster" "this" {
  cluster_identifier           = "rdsh-cluster-{{.Suffix}}"
  database_name                = "testdb"
  master_username              = "admin"
  master_password              = "Test1234!"
  node_type                    = "dc2.large"
  cluster_type                 = "single-node"
  cluster_subnet_group_name    = aws_redshift_subnet_group.this.name
  vpc_security_group_ids       = ["sg-00000000"]
  cluster_parameter_group_name = aws_redshift_parameter_group.this.name
  skip_final_snapshot          = true

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }
}

# --- Simple standalone resources ---------------------------------------------

resource "aws_redshift_authentication_profile" "this" {
  authentication_profile_name = "rdsh-auth-profile-{{.Suffix}}"
  authentication_profile_content = jsonencode({
    AllowDbUserOverride = "1"
    Client_ID           = "redshift-resources"
    App_ID              = "rdsh-app"
  })
}

resource "aws_redshift_hsm_client_certificate" "this" {
  hsm_client_certificate_identifier = "rdsh-hsm-cert-{{.Suffix}}"
}

resource "aws_redshift_hsm_configuration" "this" {
  hsm_configuration_identifier  = "rdsh-hsm-config-{{.Suffix}}"
  description                   = "rdsh hsm configuration"
  hsm_ip_address                = "10.0.0.100"
  hsm_partition_name            = "rdsh-partition"
  hsm_partition_password        = "PartitionPassw0rd!"
  hsm_server_public_certificate = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAS6gAwIBAgIUV2VaB\n-----END CERTIFICATE-----\n"
}

resource "aws_redshift_snapshot_copy_grant" "this" {
  snapshot_copy_grant_name = "rdsh-copy-grant-{{.Suffix}}"
  kms_key_id               = aws_kms_key.this.arn
}

resource "aws_redshift_snapshot_schedule" "this" {
  identifier  = "rdsh-schedule-{{.Suffix}}"
  description = "rdsh snapshot schedule"
  definitions = ["rate(12 hours)"]
}

# --- Resources attached to the cluster ---------------------------------------

resource "aws_redshift_cluster_iam_roles" "this" {
  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  iam_role_arns      = [aws_iam_role.redshift.arn]
}

resource "aws_redshift_cluster_snapshot" "this" {
  cluster_identifier  = aws_redshift_cluster.this.cluster_identifier
  snapshot_identifier = "rdsh-snapshot-{{.Suffix}}"
}

resource "aws_redshift_event_subscription" "this" {
  name          = "rdsh-events-sub-{{.Suffix}}"
  sns_topic_arn = aws_sns_topic.events.arn
  source_type   = "cluster"
  source_ids    = [aws_redshift_cluster.this.cluster_identifier]
}

resource "aws_redshift_logging" "this" {
  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  bucket_name        = aws_s3_bucket.logs.bucket
  s3_key_prefix      = "rdsh/"
}

resource "aws_redshift_scheduled_action" "this" {
  name     = "rdsh-scheduled-action-{{.Suffix}}"
  schedule = "rate(12 hours)"
  iam_role = aws_iam_role.redshift.arn

  target_action {
    pause_cluster {
      cluster_identifier = aws_redshift_cluster.this.cluster_identifier
    }
  }
}

resource "aws_redshift_snapshot_copy" "this" {
  cluster_identifier       = aws_redshift_cluster.this.cluster_identifier
  destination_region       = "us-west-2"
  snapshot_copy_grant_name = aws_redshift_snapshot_copy_grant.this.snapshot_copy_grant_name
}

resource "aws_redshift_snapshot_schedule_association" "this" {
  cluster_identifier  = aws_redshift_cluster.this.cluster_identifier
  schedule_identifier = aws_redshift_snapshot_schedule.this.id
}

resource "aws_redshift_usage_limit" "this" {
  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  feature_type       = "concurrency-scaling"
  limit_type         = "time"
  amount             = 60
}

resource "aws_redshift_resource_policy" "this" {
  resource_arn = aws_redshift_cluster_snapshot.this.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "rdsh-share"
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::111111111111:root" }
      Action    = "redshift:RestoreFromClusterSnapshot"
    }]
  })
}

resource "aws_redshift_partner" "this" {
  account_id         = "000000000000"
  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  database_name      = "testdb"
  partner_name       = "rdsh-partner"
}

resource "aws_redshift_endpoint_access" "this" {
  cluster_identifier     = aws_redshift_cluster.this.cluster_identifier
  endpoint_name          = "rdsh-endpoint-{{.Suffix}}"
  subnet_group_name      = aws_redshift_subnet_group.this.name
  vpc_security_group_ids = ["sg-00000000"]
}

resource "aws_redshift_endpoint_authorization" "this" {
  cluster_identifier = aws_redshift_cluster.this.cluster_identifier
  account            = "111111111111"
}

# --- Zero-ETL integration (Aurora -> Redshift) --------------------------------

resource "aws_db_subnet_group" "aurora" {
  name       = "rdsh-aurora-subnet-group-{{.Suffix}}"
  subnet_ids = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_rds_cluster" "aurora" {
  cluster_identifier   = "rdsh-aurora-{{.Suffix}}"
  engine               = "aurora-postgresql"
  master_username      = "admin"
  master_password      = "password123"
  db_subnet_group_name = aws_db_subnet_group.aurora.name
  skip_final_snapshot  = true
}

resource "aws_redshift_integration" "this" {
  integration_name = "rdsh-integration-{{.Suffix}}"
  source_arn       = aws_rds_cluster.aurora.arn
  target_arn       = aws_redshift_cluster.this.arn
}
