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
    elasticache = var.gopherstack_endpoint
    kafka       = var.gopherstack_endpoint
    memorydb    = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

variable "prefix" {
  description = "Resource name prefix"
  type        = string
  default     = "gopherstack-comprehensive"
}

# ---------------------------------------------------------------------------
# ElastiCache: Parameter Group, Subnet Group, Replication Group, Snapshot, User
# ---------------------------------------------------------------------------

resource "aws_elasticache_parameter_group" "redis7" {
  name   = "${var.prefix}-pg"
  family = "redis7"

  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }
}

resource "aws_elasticache_subnet_group" "main" {
  name       = "${var.prefix}-sg"
  subnet_ids = ["subnet-00000001", "subnet-00000002"]
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${var.prefix}-rg"
  description                = "Comprehensive test replication group"
  node_type                  = "cache.t3.micro"
  num_cache_clusters         = 2
  parameter_group_name       = aws_elasticache_parameter_group.redis7.name
  subnet_group_name          = aws_elasticache_subnet_group.main.name
  automatic_failover_enabled = true

  tags = {
    Environment = "test"
  }
}

resource "aws_elasticache_snapshot" "rg_snapshot" {
  name                 = "${var.prefix}-snap"
  replication_group_id = aws_elasticache_replication_group.redis.id
}

resource "aws_elasticache_user" "app" {
  user_id       = "${var.prefix}-user"
  user_name     = "appuser"
  access_string = "on ~* +@all"
  engine        = "REDIS"

  authentication_mode {
    type = "no-password-required"
  }
}

resource "aws_elasticache_user_group" "app" {
  engine        = "REDIS"
  user_group_id = "${var.prefix}-ug"
  user_ids      = [aws_elasticache_user.app.user_id]
}

# ---------------------------------------------------------------------------
# MemoryDB: ACL, Subnet Group, Cluster, Snapshot
# ---------------------------------------------------------------------------

resource "aws_memorydb_acl" "app" {
  name = "${var.prefix}-acl"

  tags = {
    Environment = "test"
  }
}

resource "aws_memorydb_subnet_group" "main" {
  name       = "${var.prefix}-mdb-sg"
  subnet_ids = ["subnet-00000001", "subnet-00000002"]

  tags = {
    Environment = "test"
  }
}

resource "aws_memorydb_cluster" "main" {
  name              = "${var.prefix}-mdb"
  node_type         = "db.r6g.large"
  acl_name          = aws_memorydb_acl.app.name
  subnet_group_name = aws_memorydb_subnet_group.main.name
  num_shards        = 1

  tags = {
    Environment = "test"
  }
}

resource "aws_memorydb_snapshot" "main" {
  name         = "${var.prefix}-mdb-snap"
  cluster_name = aws_memorydb_cluster.main.name

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# MSK: Configuration, Cluster with SASL/IAM Authentication
# ---------------------------------------------------------------------------

resource "aws_msk_configuration" "main" {
  name              = "${var.prefix}-config"
  kafka_versions    = ["3.5.1"]
  server_properties = "auto.create.topics.enable=true\nlog.retention.hours=168"
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.prefix}-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 1

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = ["subnet-00000000"]
    security_groups = ["sg-00000000"]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = 1
  }

  tags = {
    Environment = "test"
  }
}
