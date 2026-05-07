terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    rds = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# DB subnet group
# ---------------------------------------------------------------------------

resource "aws_db_subnet_group" "main" {
  name        = "rds-comp-subnet-group"
  description = "Subnet group for comprehensive RDS terraform tests"
  subnet_ids  = ["subnet-00000001", "subnet-00000002"]
}

# ---------------------------------------------------------------------------
# DB parameter group with custom parameters
# ---------------------------------------------------------------------------

resource "aws_db_parameter_group" "postgres" {
  name        = "rds-comp-pg-postgres"
  family      = "postgres15"
  description = "Custom PostgreSQL parameter group"

  parameter {
    name  = "log_connections"
    value = "1"
  }

  parameter {
    name  = "log_disconnections"
    value = "1"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
}

# ---------------------------------------------------------------------------
# DB instance using the custom parameter group
# ---------------------------------------------------------------------------

resource "aws_db_instance" "primary" {
  identifier              = "rds-comp-primary"
  engine                  = "postgres"
  engine_version          = "15.5"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  db_name                 = "compdb"
  username                = "admin"
  password                = "Password1234!"
  parameter_group_name    = aws_db_parameter_group.postgres.name
  db_subnet_group_name    = aws_db_subnet_group.main.name
  skip_final_snapshot     = true
  backup_retention_period = 7
}

# ---------------------------------------------------------------------------
# Read replica of the primary instance
# ---------------------------------------------------------------------------

resource "aws_db_instance" "replica" {
  identifier          = "rds-comp-replica"
  replicate_source_db = aws_db_instance.primary.identifier
  instance_class      = "db.t3.micro"
  skip_final_snapshot = true
}

# ---------------------------------------------------------------------------
# Aurora cluster parameter group
# ---------------------------------------------------------------------------

resource "aws_rds_cluster_parameter_group" "aurora" {
  name        = "rds-comp-cluster-pg"
  family      = "aurora-postgresql15"
  description = "Aurora PostgreSQL cluster parameter group"

  parameter {
    name  = "log_connections"
    value = "1"
  }
}

# ---------------------------------------------------------------------------
# Aurora cluster
# ---------------------------------------------------------------------------

resource "aws_rds_cluster" "aurora" {
  cluster_identifier               = "rds-comp-aurora"
  engine                           = "aurora-postgresql"
  engine_mode                      = "provisioned"
  database_name                    = "auroradb"
  master_username                  = "admin"
  master_password                  = "Password1234!"
  db_subnet_group_name             = aws_db_subnet_group.main.name
  db_cluster_parameter_group_name  = aws_rds_cluster_parameter_group.aurora.name
  skip_final_snapshot              = true

  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 8.0
  }
}

# ---------------------------------------------------------------------------
# Event subscription for the Aurora cluster
# ---------------------------------------------------------------------------

resource "aws_db_event_subscription" "cluster_events" {
  name      = "rds-comp-cluster-events"
  sns_topic = "arn:aws:sns:us-east-1:000000000000:rds-events"

  source_type = "db-cluster"
  source_ids  = [aws_rds_cluster.aurora.id]

  event_categories = [
    "availability",
    "deletion",
    "failover",
    "maintenance",
  ]
}

# ---------------------------------------------------------------------------
# Event subscription for DB instances
# ---------------------------------------------------------------------------

resource "aws_db_event_subscription" "instance_events" {
  name      = "rds-comp-instance-events"
  sns_topic = "arn:aws:sns:us-east-1:000000000000:rds-instance-events"

  source_type = "db-instance"

  event_categories = [
    "availability",
    "backup",
    "maintenance",
  ]
}

# ---------------------------------------------------------------------------
# Cluster snapshot
# ---------------------------------------------------------------------------

resource "aws_db_cluster_snapshot" "aurora_snap" {
  db_cluster_identifier          = aws_rds_cluster.aurora.id
  db_cluster_snapshot_identifier = "rds-comp-aurora-snap"
}

# ---------------------------------------------------------------------------
# Reserved instance data source (read-only — no resource to create)
# ---------------------------------------------------------------------------

data "aws_db_instance_offering" "t3_micro_postgres" {
  db_instance_class = "db.t3.micro"
  engine            = "postgres"
  license_model     = "postgresql-license"
}

# ---------------------------------------------------------------------------
# Certificates data source
# ---------------------------------------------------------------------------

data "aws_rds_certificate" "latest" {
  latest_valid_till = true
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "primary_instance_endpoint" {
  value = aws_db_instance.primary.address
}

output "replica_instance_endpoint" {
  value = aws_db_instance.replica.address
}

output "aurora_cluster_endpoint" {
  value = aws_rds_cluster.aurora.endpoint
}

output "cluster_event_subscription_arn" {
  value = aws_db_event_subscription.cluster_events.arn
}

output "cluster_snapshot_id" {
  value = aws_db_cluster_snapshot.aurora_snap.id
}

output "certificate_id" {
  value = data.aws_rds_certificate.latest.id
}
