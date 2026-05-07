terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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
    ecs = "http://localhost:4566"
  }
}

# --------------------------------------------------------------------------
# Cluster with capacity providers
# --------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = "gopherstack-comprehensive-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 2
    base              = 0
  }
}

# --------------------------------------------------------------------------
# Task definition with resource limits on containers
# --------------------------------------------------------------------------

resource "aws_ecs_task_definition" "api" {
  family                   = "gopherstack-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"

  container_definitions = jsonencode([
    {
      name      = "api"
      image     = "nginx:latest"
      essential = true
      cpu       = 256
      memory    = 512
      portMappings = [
        {
          containerPort = 8080
          hostPort      = 8080
          protocol      = "tcp"
          name          = "api-port"
        }
      ]
      environment = [
        {
          name  = "ENV"
          value = "production"
        },
        {
          name  = "PORT"
          value = "8080"
        }
      ]
    }
  ])
}

# --------------------------------------------------------------------------
# Service with service connect configuration
# --------------------------------------------------------------------------

resource "aws_ecs_service" "api" {
  name            = "gopherstack-api-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }

  service_connect_configuration {
    enabled   = true
    namespace = "gopherstack.local"

    service {
      port_name      = "api-port"
      discovery_name = "api"

      client_alias {
        port     = 8080
        dns_name = "api.gopherstack.local"
      }
    }
  }
}

# --------------------------------------------------------------------------
# Account settings
# --------------------------------------------------------------------------

resource "aws_ecs_account_setting_default" "container_insights" {
  name  = "containerInsights"
  value = "enabled"
}

resource "aws_ecs_account_setting_default" "task_long_arn" {
  name  = "taskLongArnFormat"
  value = "enabled"
}

# --------------------------------------------------------------------------
# Data sources
# --------------------------------------------------------------------------

data "aws_ecs_cluster" "main" {
  cluster_name = aws_ecs_cluster.main.name
}

# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------

output "cluster_arn" {
  value = aws_ecs_cluster.main.arn
}

output "cluster_status" {
  value = data.aws_ecs_cluster.main.status
}

output "cluster_capacity_providers" {
  value = aws_ecs_cluster_capacity_providers.main.capacity_providers
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.api.arn
}

output "service_name" {
  value = aws_ecs_service.api.name
}
