# --- IAM / KMS / S3 / SNS / Secrets ------------------------------------------

resource "aws_iam_role" "rds" {
  name = "rdsu-rds-role-{{.Suffix}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["rds.amazonaws.com", "export.rds.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_kms_key" "this" {
  description = "rdsu kms key"
}

resource "aws_s3_bucket" "export" {
  bucket = "rdsu-export-bucket-{{.Suffix}}"
}

resource "aws_sns_topic" "events" {
  name = "rdsu-events-{{.Suffix}}"
}

resource "aws_secretsmanager_secret" "proxy" {
  name = "rdsu-proxy-secret-{{.Suffix}}"
}

resource "aws_secretsmanager_secret_version" "proxy" {
  secret_id     = aws_secretsmanager_secret.proxy.id
  secret_string = jsonencode({ username = "admin", password = "password123" })
}

# --- DB subnet group / parameter group / option group ------------------------
# Subnet/security-group IDs are literal placeholders (as in the docdb/neptune
# fixtures) rather than real aws_vpc/aws_subnet/aws_security_group resources:
# RDS's subnet group and proxy backends never cross-check against EC2 state,
# and provisioning a real VPC here only adds a slow, unrelated EC2 lifecycle.

resource "aws_db_subnet_group" "this" {
  name       = "rdsu-subnet-group-{{.Suffix}}"
  subnet_ids = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_db_parameter_group" "mysql" {
  name   = "rdsu-mysql-params-{{.Suffix}}"
  family = "mysql8.0"

  parameter {
    name  = "character_set_server"
    value = "utf8mb4"
  }
}

resource "aws_db_option_group" "mysql" {
  name                 = "rdsu-mysql-options-{{.Suffix}}"
  engine_name          = "mysql"
  major_engine_version = "8.0"
}

# --- DB instances --------------------------------------------------------------

resource "aws_db_instance" "mysql" {
  identifier           = "rdsu-mysql-{{.Suffix}}"
  engine               = "mysql"
  engine_version       = "8.0"
  instance_class       = "db.t3.micro"
  username             = "admin"
  password             = "password123"
  allocated_storage    = 20
  db_subnet_group_name = aws_db_subnet_group.this.name
  parameter_group_name = aws_db_parameter_group.mysql.name
  option_group_name    = aws_db_option_group.mysql.name
  skip_final_snapshot  = true
}

resource "aws_db_instance" "pg" {
  identifier           = "rdsu-pg-{{.Suffix}}"
  engine               = "postgres"
  instance_class       = "db.t3.micro"
  username             = "admin"
  password             = "password123"
  db_name              = "testdb"
  allocated_storage    = 20
  db_subnet_group_name = aws_db_subnet_group.this.name
  skip_final_snapshot  = true
}

# --- Snapshots -------------------------------------------------------------------

resource "aws_db_snapshot" "mysql" {
  db_instance_identifier = aws_db_instance.mysql.identifier
  db_snapshot_identifier = "rdsu-mysql-snap-{{.Suffix}}"
}

resource "aws_db_snapshot_copy" "mysql" {
  source_db_snapshot_identifier = aws_db_snapshot.mysql.db_snapshot_identifier
  target_db_snapshot_identifier = "rdsu-mysql-snap-copy-{{.Suffix}}"
}

# --- Role association / event subscription / instance state / certificate ------

resource "aws_db_instance_role_association" "pg" {
  db_instance_identifier = aws_db_instance.pg.identifier
  feature_name           = "s3Import"
  role_arn               = aws_iam_role.rds.arn
}

resource "aws_db_event_subscription" "this" {
  name        = "rdsu-events-sub-{{.Suffix}}"
  sns_topic   = aws_sns_topic.events.arn
  source_type = "db-instance"
  source_ids  = [aws_db_instance.mysql.identifier]
}

resource "aws_rds_instance_state" "pg" {
  identifier = aws_db_instance.pg.identifier
  state      = "available"
}

resource "aws_rds_certificate" "this" {
  certificate_identifier = "rds-ca-rsa2048-g1"
}

# --- DB proxy family -------------------------------------------------------------

resource "aws_db_proxy" "this" {
  name                   = "rdsu-proxy-{{.Suffix}}"
  engine_family          = "POSTGRESQL"
  role_arn               = aws_iam_role.rds.arn
  vpc_subnet_ids         = ["subnet-00000000", "subnet-11111111"]
  vpc_security_group_ids = ["sg-00000000"]

  auth {
    auth_scheme = "SECRETS"
    iam_auth    = "DISABLED"
    secret_arn  = aws_secretsmanager_secret.proxy.arn
  }
}

resource "aws_db_proxy_default_target_group" "this" {
  db_proxy_name = aws_db_proxy.this.name
}

resource "aws_db_proxy_endpoint" "this" {
  db_proxy_name          = aws_db_proxy.this.name
  db_proxy_endpoint_name = "rdsu-proxy-endpoint-{{.Suffix}}"
  vpc_subnet_ids         = ["subnet-00000000", "subnet-11111111"]
}

resource "aws_db_proxy_target" "this" {
  db_proxy_name          = aws_db_proxy.this.name
  target_group_name      = aws_db_proxy_default_target_group.this.name
  db_instance_identifier = aws_db_instance.pg.identifier
}

# --- Aurora cluster family -------------------------------------------------

resource "aws_rds_cluster" "aurora" {
  cluster_identifier       = "rdsu-aurora-{{.Suffix}}"
  engine                   = "aurora-postgresql"
  master_username          = "admin"
  master_password          = "password123"
  db_subnet_group_name     = aws_db_subnet_group.this.name
  cluster_scalability_type = "limitless"
  skip_final_snapshot      = true
}

resource "aws_rds_cluster_instance" "aurora" {
  identifier         = "rdsu-aurora-inst-{{.Suffix}}"
  cluster_identifier = aws_rds_cluster.aurora.id
  engine             = aws_rds_cluster.aurora.engine
  instance_class     = "db.t3.medium"
}

resource "aws_rds_cluster_parameter_group" "aurora" {
  name   = "rdsu-aurora-cpg-{{.Suffix}}"
  family = "aurora-postgresql16"

  parameter {
    name  = "log_statement"
    value = "all"
  }
}

resource "aws_rds_cluster_endpoint" "aurora" {
  cluster_identifier          = aws_rds_cluster.aurora.id
  cluster_endpoint_identifier = "rdsu-aurora-reader-{{.Suffix}}"
  custom_endpoint_type        = "READER"

  depends_on = [aws_rds_cluster_instance.aurora]
}

resource "aws_rds_cluster_role_association" "aurora" {
  db_cluster_identifier = aws_rds_cluster.aurora.id
  feature_name          = "s3Import"
  role_arn              = aws_iam_role.rds.arn
}

resource "aws_db_cluster_snapshot" "aurora" {
  db_cluster_identifier          = aws_rds_cluster.aurora.id
  db_cluster_snapshot_identifier = "rdsu-aurora-snap-{{.Suffix}}"
}

# --- Global cluster / export task / activity stream / backups replication --

resource "aws_rds_global_cluster" "this" {
  global_cluster_identifier = "rdsu-global-{{.Suffix}}"
  engine                    = "aurora-postgresql"
  database_name             = "testdb"
  storage_encrypted         = true
}

resource "aws_rds_export_task" "this" {
  export_task_identifier = "rdsu-export-{{.Suffix}}"
  source_arn             = aws_db_cluster_snapshot.aurora.db_cluster_snapshot_arn
  s3_bucket_name         = aws_s3_bucket.export.bucket
  iam_role_arn           = aws_iam_role.rds.arn
  kms_key_id             = aws_kms_key.this.arn
}

resource "aws_rds_cluster_activity_stream" "aurora" {
  resource_arn = aws_rds_cluster.aurora.arn
  kms_key_id   = aws_kms_key.this.arn
  mode         = "async"

  depends_on = [aws_rds_cluster_instance.aurora]
}

resource "aws_db_instance_automated_backups_replication" "mysql" {
  source_db_instance_arn = aws_db_instance.mysql.arn
  kms_key_id             = aws_kms_key.this.arn
}

resource "aws_rds_shard_group" "this" {
  db_shard_group_identifier = "rdsu-shard-group-{{.Suffix}}"
  db_cluster_identifier     = aws_rds_cluster.aurora.id
  max_acu                   = 128
  min_acu                   = 2

  depends_on = [aws_rds_cluster_instance.aurora]
}

# --- Zero-ETL integration (Aurora -> Redshift) ------------------------------

resource "aws_redshift_cluster" "target" {
  cluster_identifier  = "rdsu-redshift-target-{{.Suffix}}"
  database_name       = "targetdb"
  master_username     = "admin"
  master_password     = "Test1234!"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true
}

resource "aws_rds_integration" "this" {
  integration_name = "rdsu-integration-{{.Suffix}}"
  source_arn       = aws_rds_cluster.aurora.arn
  target_arn       = aws_redshift_cluster.target.arn

  depends_on = [aws_rds_cluster_instance.aurora]
}
