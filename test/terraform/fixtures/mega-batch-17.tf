##############################################################################
# SSM
##############################################################################

resource "aws_iam_role" "ssm_activation" {
  name = "mega-batch-17-ssm-activation-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ssm.amazonaws.com" }
    }]
  })
}

resource "aws_ssm_activation" "example" {
  name               = "mega-batch-17-activation"
  iam_role           = aws_iam_role.ssm_activation.id
  registration_limit = 5
}

resource "aws_ssm_document" "example" {
  name          = "mega-batch-17-document"
  document_type = "Command"

  content = jsonencode({
    schemaVersion = "2.2"
    description   = "mega batch 17 document"
    mainSteps = [{
      action = "aws:runShellScript"
      name   = "runShellScript"
      inputs = {
        runCommand = ["echo hello"]
      }
    }]
  })
}

resource "aws_ssm_association" "example" {
  name = aws_ssm_document.example.name

  targets {
    key    = "tag:Name"
    values = ["mega-batch-17"]
  }
}

resource "aws_ssm_maintenance_window" "example" {
  name     = "mega-batch-17-window"
  schedule = "cron(0 16 ? * TUE *)"
  duration = 3
  cutoff   = 1
}

resource "aws_ssm_maintenance_window_target" "example" {
  window_id     = aws_ssm_maintenance_window.example.id
  name          = "mega-batch-17-window-target"
  resource_type = "INSTANCE"

  targets {
    key    = "tag:Name"
    values = ["mega-batch-17"]
  }
}

resource "aws_s3_bucket" "ssm_task_output" {
  bucket        = "mega-batch-17-ssm-task-output"
  force_destroy = true
}

resource "aws_iam_role" "ssm_task" {
  name = "mega-batch-17-ssm-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ssm.amazonaws.com" }
    }]
  })
}

resource "aws_ssm_maintenance_window_task" "example" {
  window_id       = aws_ssm_maintenance_window.example.id
  task_type       = "RUN_COMMAND"
  task_arn        = "AWS-RunShellScript"
  priority        = 1
  max_concurrency = "2"
  max_errors      = "1"

  targets {
    key    = "WindowTargetIds"
    values = [aws_ssm_maintenance_window_target.example.id]
  }

  task_invocation_parameters {
    run_command_parameters {
      output_s3_bucket     = aws_s3_bucket.ssm_task_output.id
      output_s3_key_prefix = "output"
      service_role_arn     = aws_iam_role.ssm_task.arn
      timeout_seconds      = 600

      parameter {
        name   = "commands"
        values = ["echo hello"]
      }
    }
  }
}

resource "aws_ssm_patch_baseline" "example" {
  name             = "mega-batch-17-patch-baseline"
  operating_system = "AMAZON_LINUX_2"

  approval_rule {
    approve_after_days = 7

    patch_filter {
      key    = "CLASSIFICATION"
      values = ["Security"]
    }
  }
}

resource "aws_ssm_default_patch_baseline" "example" {
  baseline_id      = aws_ssm_patch_baseline.example.id
  operating_system = "AMAZON_LINUX_2"
}

resource "aws_ssm_patch_group" "example" {
  baseline_id = aws_ssm_patch_baseline.example.id
  patch_group = "mega-batch-17-patch-group"
}

resource "aws_s3_bucket" "ssm_sync" {
  bucket        = "mega-batch-17-ssm-sync"
  force_destroy = true
}

resource "aws_ssm_resource_data_sync" "example" {
  name = "mega-batch-17-sync"

  s3_destination {
    bucket_name = aws_s3_bucket.ssm_sync.bucket
    region      = "us-east-1"
  }
}

resource "aws_ssm_service_setting" "example" {
  setting_id    = "arn:aws:ssm:us-east-1:000000000000:servicesetting/ssm/parameter-store/high-throughput-enabled"
  setting_value = "true"
}

##############################################################################
# Backup
##############################################################################

resource "aws_iam_role" "backup" {
  name = "mega-batch-17-backup-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "backup.amazonaws.com" }
    }]
  })
}

resource "aws_backup_vault" "example" {
  name = "mega-batch-17-vault"
}

resource "aws_backup_plan" "example" {
  name = "mega-batch-17-plan"

  rule {
    rule_name         = "mega-batch-17-rule"
    target_vault_name = aws_backup_vault.example.name
    schedule          = "cron(0 12 * * ? *)"
  }
}

resource "aws_backup_selection" "example" {
  iam_role_arn = aws_iam_role.backup.arn
  name         = "mega-batch-17-selection"
  plan_id      = aws_backup_plan.example.id

  resources = ["*"]
}

resource "aws_backup_framework" "example" {
  name        = "mega_batch_17_framework"
  description = "mega batch 17 framework"

  control {
    name = "BACKUP_RECOVERY_POINT_MINIMUM_RETENTION_CHECK"

    input_parameter {
      name  = "requiredRetentionDays"
      value = "35"
    }
  }
}

resource "aws_s3_bucket" "backup_reports" {
  bucket        = "mega-batch-17-backup-reports"
  force_destroy = true
}

resource "aws_backup_report_plan" "example" {
  name        = "mega_batch_17_report_plan"
  description = "mega batch 17 report plan"

  report_delivery_channel {
    s3_bucket_name = aws_s3_bucket.backup_reports.bucket
    formats        = ["CSV"]
  }

  report_setting {
    report_template = "BACKUP_JOB_REPORT"
  }
}

resource "aws_backup_restore_testing_plan" "example" {
  name = "mega_batch_17_restore_testing_plan"

  recovery_point_selection {
    algorithm            = "LATEST_WITHIN_WINDOW"
    include_vaults       = ["*"]
    recovery_point_types = ["SNAPSHOT"]
  }

  schedule_expression = "cron(0 12 * * ? *)"
}

resource "aws_backup_restore_testing_selection" "example" {
  name                       = "mega_batch_17_restore_testing_selection"
  restore_testing_plan_name = aws_backup_restore_testing_plan.example.name
  iam_role_arn               = aws_iam_role.backup.arn
  protected_resource_type    = "EC2"

  protected_resource_conditions {
    string_equals {
      key   = "aws:ResourceTag/backup"
      value = "true"
    }
  }
}

resource "aws_backup_logically_air_gapped_vault" "example" {
  name               = "mega-batch-17-lag-vault"
  max_retention_days = 100
  min_retention_days = 7
}

resource "aws_backup_vault_lock_configuration" "example" {
  backup_vault_name   = aws_backup_vault.example.name
  changeable_for_days = 3
  max_retention_days  = 365
  min_retention_days  = 7
}

resource "aws_sns_topic" "backup" {
  name = "mega-batch-17-backup-topic"
}

resource "aws_backup_vault_notifications" "example" {
  backup_vault_name   = aws_backup_vault.example.name
  sns_topic_arn       = aws_sns_topic.backup.arn
  backup_vault_events = ["BACKUP_JOB_STARTED", "BACKUP_JOB_COMPLETED"]
}

resource "aws_backup_vault_policy" "example" {
  backup_vault_name = aws_backup_vault.example.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "default"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "backup:DescribeBackupVault"
      Resource  = "*"
    }]
  })
}

resource "aws_backup_global_settings" "example" {
  global_settings = {
    "isCrossAccountBackupEnabled" = "true"
  }
}

resource "aws_backup_region_settings" "example" {
  resource_type_opt_in_preference = {
    "DynamoDB" = true
    "EFS"      = true
  }

  resource_type_management_preference = {
    "DynamoDB" = false
  }
}
