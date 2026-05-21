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
# DB subnet group (required for cluster creation)
# ---------------------------------------------------------------------------

resource "aws_db_subnet_group" "main" {
  name        = "rds-tf-subnet-group"
  description = "Subnet group for RDS terraform tests"
  subnet_ids  = ["subnet-00000001", "subnet-00000002"]
}

# ---------------------------------------------------------------------------
# Aurora cluster parameter group
# ---------------------------------------------------------------------------

resource "aws_rds_cluster_parameter_group" "aurora_pg" {
  name        = "rds-tf-cluster-pg"
  family      = "aurora-postgresql15"
  description = "Aurora PostgreSQL cluster parameter group"

  parameter {
    name  = "log_connections"
    value = "1"
  }
}

# ---------------------------------------------------------------------------
# Aurora Serverless v2 cluster
# ---------------------------------------------------------------------------

resource "aws_rds_cluster" "aurora" {
  cluster_identifier      = "rds-tf-aurora-cluster"
  engine                  = "aurora-postgresql"
  engine_mode             = "provisioned"
  database_name           = "testdb"
  master_username         = "admin"
  master_password         = "Password1234!"
  db_subnet_group_name    = aws_db_subnet_group.main.name
  db_cluster_parameter_group_name = aws_rds_cluster_parameter_group.aurora_pg.name
  skip_final_snapshot     = true

  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 4.0
  }
}

# ---------------------------------------------------------------------------
# DB cluster snapshot
# ---------------------------------------------------------------------------

resource "aws_db_cluster_snapshot" "aurora" {
  db_cluster_identifier          = aws_rds_cluster.aurora.id
  db_cluster_snapshot_identifier = "rds-tf-aurora-snapshot"
}

# ---------------------------------------------------------------------------
# RDS Proxy
# ---------------------------------------------------------------------------

resource "aws_db_proxy" "main" {
  name                   = "rds-tf-proxy"
  debug_logging          = false
  engine_family          = "POSTGRESQL"
  idle_client_timeout    = 1800
  require_tls            = false
  role_arn               = "arn:aws:iam::000000000000:role/rds-proxy-role"
  vpc_subnet_ids         = ["subnet-00000001", "subnet-00000002"]

  auth {
    auth_scheme = "SECRETS"
    iam_auth    = "DISABLED"
    secret_arn  = "arn:aws:secretsmanager:us-east-1:000000000000:secret:rds-proxy-secret"
  }
}

# ---------------------------------------------------------------------------
# Blue/Green deployment
# ---------------------------------------------------------------------------

resource "aws_rds_cluster" "blue" {
  cluster_identifier   = "rds-tf-blue-cluster"
  engine               = "aurora-postgresql"
  database_name        = "bluedb"
  master_username      = "admin"
  master_password      = "Password1234!"
  db_subnet_group_name = aws_db_subnet_group.main.name
  skip_final_snapshot  = true
}

resource "aws_rds_blue_green_deployment" "main" {
  blue_green_deployment_name = "rds-tf-bgd"
  source                     = aws_rds_cluster.blue.arn
}

# ---------------------------------------------------------------------------
# Aurora Limitless DB shard group (batch-1)
# ---------------------------------------------------------------------------

resource "aws_rds_shard_group" "main" {
  db_shard_group_identifier = "rds-tf-shard-group"
  db_cluster_identifier     = aws_rds_cluster.aurora.id
  max_acu                   = 64
}

# ---------------------------------------------------------------------------
# RDS Integration (zero-ETL to Redshift, batch-1)
# ---------------------------------------------------------------------------

resource "aws_rds_integration" "main" {
  integration_name = "rds-tf-integration"
  source_arn       = aws_rds_cluster.aurora.arn
  target_arn       = "arn:aws:redshift:us-east-1:000000000000:namespace:rds-tf-redshift-ns"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "aurora_cluster_endpoint" {
  value = aws_rds_cluster.aurora.endpoint
}

output "proxy_endpoint" {
  value = aws_db_proxy.main.endpoint
}

output "blue_green_deployment_id" {
  value = aws_rds_blue_green_deployment.main.id
}

output "shard_group_id" {
  value = aws_rds_shard_group.main.id
}

output "integration_arn" {
  value = aws_rds_integration.main.arn
}
