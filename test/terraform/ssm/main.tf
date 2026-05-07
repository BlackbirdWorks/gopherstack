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
    ssm = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SSM Parameter Store
# ---------------------------------------------------------------------------

resource "aws_ssm_parameter" "app_config" {
  name        = "/myapp/config/database_url"
  type        = "String"
  value       = "postgres://localhost:5432/myapp"
  description = "Application database URL"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

resource "aws_ssm_parameter" "secret" {
  name  = "/myapp/secrets/api_key"
  type  = "SecureString"
  value = "supersecretvalue"
}

# ---------------------------------------------------------------------------
# SSM Maintenance Window
# ---------------------------------------------------------------------------

resource "aws_ssm_maintenance_window" "weekly" {
  name                       = "weekly-maintenance"
  schedule                   = "cron(0 2 ? * SUN *)"
  duration                   = 4
  cutoff                     = 1
  allow_unassociated_targets = true

  tags = {
    Environment = "test"
  }
}

resource "aws_ssm_maintenance_window" "nightly" {
  name     = "nightly-maintenance"
  schedule = "cron(0 3 * * ? *)"
  duration = 2
  cutoff   = 0
}

resource "aws_ssm_maintenance_window_target" "instances" {
  window_id     = aws_ssm_maintenance_window.weekly.id
  name          = "all-managed-instances"
  resource_type = "INSTANCE"

  targets {
    key    = "tag:Environment"
    values = ["test"]
  }
}

resource "aws_ssm_maintenance_window_task" "run_script" {
  window_id = aws_ssm_maintenance_window.weekly.id
  task_arn  = "AWS-RunShellScript"
  task_type = "RUN_COMMAND"
  priority  = 10

  targets {
    key    = "WindowTargetIds"
    values = [aws_ssm_maintenance_window_target.instances.id]
  }

  task_invocation_parameters {
    run_command_parameters {
      parameter {
        name   = "commands"
        values = ["echo hello"]
      }
    }
  }
}

# ---------------------------------------------------------------------------
# SSM Patch Baseline
# ---------------------------------------------------------------------------

resource "aws_ssm_patch_baseline" "linux" {
  name             = "linux-security-patches"
  description      = "Baseline for Linux security patches"
  operating_system = "AMAZON_LINUX_2"

  approval_rule {
    approve_after_days = 7

    patch_filter {
      key    = "CLASSIFICATION"
      values = ["Security"]
    }

    patch_filter {
      key    = "SEVERITY"
      values = ["Critical", "High"]
    }
  }

  tags = {
    Environment = "test"
  }
}

resource "aws_ssm_patch_baseline" "windows" {
  name             = "windows-critical-patches"
  operating_system = "WINDOWS"

  approved_patches = ["KB1234567", "KB7654321"]
}

# ---------------------------------------------------------------------------
# SSM Document
# ---------------------------------------------------------------------------

resource "aws_ssm_document" "run_command" {
  name            = "gopherstack-run-command"
  document_type   = "Command"
  document_format = "JSON"

  content = jsonencode({
    schemaVersion = "2.2"
    description   = "Run a shell command"
    parameters = {
      commands = {
        type = "StringList"
      }
    }
    mainSteps = [{
      action = "aws:runShellScript"
      name   = "runShellScript"
      inputs = {
        commands = ["{{ commands }}"]
      }
    }]
  })

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# SSM Association
# ---------------------------------------------------------------------------

resource "aws_ssm_association" "patch_linux" {
  name = aws_ssm_document.run_command.name

  targets {
    key    = "tag:OS"
    values = ["Linux"]
  }

  parameters = {
    commands = "echo 'patching'"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "parameter_name" {
  value = aws_ssm_parameter.app_config.name
}

output "maintenance_window_id" {
  value = aws_ssm_maintenance_window.weekly.id
}

output "patch_baseline_id" {
  value = aws_ssm_patch_baseline.linux.id
}

output "document_name" {
  value = aws_ssm_document.run_command.name
}

output "window_target_id" {
  value = aws_ssm_maintenance_window_target.instances.id
}
