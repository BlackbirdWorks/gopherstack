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
    sfn = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# IAM role (stub — gopherstack does not enforce IAM)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "sfn" {
  name = "gopherstack-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

# ---------------------------------------------------------------------------
# STANDARD state machine
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "standard" {
  name     = "gopherstack-sfn-standard"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = jsonencode({
    StartAt = "Hello"
    States = {
      Hello = {
        Type   = "Pass"
        Result = { message = "hello from gopherstack" }
        End    = true
      }
    }
  })

  tags = {
    Environment = "test"
    Service     = "gopherstack"
  }
}

# ---------------------------------------------------------------------------
# EXPRESS state machine
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "express" {
  name     = "gopherstack-sfn-express"
  role_arn = aws_iam_role.sfn.arn
  type     = "EXPRESS"

  definition = jsonencode({
    StartAt = "Process"
    States = {
      Process = {
        Type   = "Pass"
        Result = { processed = true }
        End    = true
      }
    }
  })
}

# ---------------------------------------------------------------------------
# State machine version + alias
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine_version" "v1" {
  state_machine_arn = aws_sfn_state_machine.standard.arn
  description       = "Version 1"
}

resource "aws_sfn_alias" "live" {
  name        = "live"
  description = "Live alias pointing to version 1"

  routing_configuration {
    state_machine_version_arn = aws_sfn_state_machine_version.v1.versioned_arn
    weight                    = 100
  }
}

# ---------------------------------------------------------------------------
# Map-state machine (exercises ItemBatcher parsing)
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "map_batcher" {
  name     = "gopherstack-sfn-map-batcher"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = jsonencode({
    StartAt = "BatchMap"
    States = {
      BatchMap = {
        Type           = "Map"
        MaxConcurrency = 2
        ItemBatcher = {
          MaxItemsPerBatch = 5
        }
        ItemProcessor = {
          StartAt = "ProcessBatch"
          States = {
            ProcessBatch = {
              Type = "Pass"
              End  = true
            }
          }
        }
        End = true
      }
    }
  })
}

# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------

resource "aws_sfn_activity" "worker" {
  name = "gopherstack-sfn-worker"

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# State machine with tracing + logging configuration (X-Ray + CloudWatch Logs)
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "tracing" {
  name     = "gopherstack-sfn-tracing"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  tracing_configuration {
    enabled = true
  }

  logging_configuration {
    level                  = "ALL"
    include_execution_data = true

    log_destination = "arn:aws:logs:us-east-1:000000000000:log-group:/aws/vendedlogs/states/gopherstack:*"
  }

  definition = jsonencode({
    StartAt = "Hello"
    States = {
      Hello = {
        Type   = "Pass"
        Result = { traced = true }
        End    = true
      }
    }
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "standard_state_machine_arn" {
  value = aws_sfn_state_machine.standard.arn
}

output "express_state_machine_arn" {
  value = aws_sfn_state_machine.express.arn
}

output "map_batcher_state_machine_arn" {
  value = aws_sfn_state_machine.map_batcher.arn
}

output "activity_arn" {
  value = aws_sfn_activity.worker.id
}

output "alias_arn" {
  value = aws_sfn_alias.live.arn
}

output "tracing_state_machine_arn" {
  value = aws_sfn_state_machine.tracing.arn
}
