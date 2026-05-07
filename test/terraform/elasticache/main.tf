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
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Cache Parameter Group
# ---------------------------------------------------------------------------

resource "aws_elasticache_parameter_group" "redis7" {
  name   = "gopherstack-test-redis7"
  family = "redis7"

  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }
}

# ---------------------------------------------------------------------------
# Cache Subnet Group
# ---------------------------------------------------------------------------

resource "aws_elasticache_subnet_group" "main" {
  name       = "gopherstack-test-subnet-group"
  subnet_ids = ["subnet-00000001", "subnet-00000002"]
}

# ---------------------------------------------------------------------------
# Cache Cluster (Memcached)
# ---------------------------------------------------------------------------

resource "aws_elasticache_cluster" "memcached" {
  cluster_id           = "gopherstack-test-memcached"
  engine               = "memcached"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.memcached1.6"
  subnet_group_name    = aws_elasticache_subnet_group.main.name
}

# ---------------------------------------------------------------------------
# Replication Group (Redis)
# ---------------------------------------------------------------------------

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "gopherstack-test-redis"
  description          = "gopherstack test redis replication group"
  node_type            = "cache.t3.micro"
  num_cache_clusters   = 1
  parameter_group_name = aws_elasticache_parameter_group.redis7.name
  subnet_group_name    = aws_elasticache_subnet_group.main.name

  automatic_failover_enabled = false
  multi_az_enabled           = false
}

# ---------------------------------------------------------------------------
# Serverless Cache (Redis)
# ---------------------------------------------------------------------------

resource "aws_elasticache_serverless_cache" "redis" {
  engine = "redis"
  name   = "gopherstack-test-serverless-redis"
}

# ---------------------------------------------------------------------------
# Serverless Cache (Memcached)
# ---------------------------------------------------------------------------

resource "aws_elasticache_serverless_cache" "memcached" {
  engine = "memcached"
  name   = "gopherstack-test-serverless-memcached"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "cluster_id" {
  value = aws_elasticache_cluster.memcached.id
}

output "replication_group_id" {
  value = aws_elasticache_replication_group.redis.id
}

output "serverless_redis_endpoint" {
  value = aws_elasticache_serverless_cache.redis.endpoint[0].address
}

output "serverless_memcached_endpoint" {
  value = aws_elasticache_serverless_cache.memcached.endpoint[0].address
}
