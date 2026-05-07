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
  name = "gopherstack-test-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled"
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
}

# --------------------------------------------------------------------------
# Task definition with placement constraints
# --------------------------------------------------------------------------

resource "aws_ecs_task_definition" "web" {
  family                   = "gopherstack-web"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"

  container_definitions = jsonencode([
    {
      name      = "nginx"
      image     = "nginx:latest"
      essential = true
      cpu       = 256
      memory    = 512
      portMappings = [
        {
          containerPort = 80
          hostPort      = 80
          protocol      = "tcp"
        }
      ]
    }
  ])
}

# --------------------------------------------------------------------------
# Service with deployment circuit breaker and capacity provider strategy
# --------------------------------------------------------------------------

resource "aws_ecs_service" "web" {
  name            = "gopherstack-web-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.web.arn
  desired_count   = 1

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 2
    base              = 0
  }

  placement_strategy {
    type  = "spread"
    field = "az"
  }
}

# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------

output "cluster_arn" {
  value = aws_ecs_cluster.main.arn
}

output "service_name" {
  value = aws_ecs_service.web.name
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.web.arn
}
