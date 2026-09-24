##############################################################################
# EFS: file system with an access point, a backup policy, and a
# resource-based file system policy.
##############################################################################

resource "aws_efs_file_system" "mb46" {
  tags = {
    Name = "mega-batch-46-efs"
  }
}

resource "aws_efs_access_point" "mb46" {
  file_system_id = aws_efs_file_system.mb46.id

  posix_user {
    gid = 1000
    uid = 1000
  }

  root_directory {
    path = "/mega-batch-46"

    creation_info {
      owner_gid   = 1000
      owner_uid   = 1000
      permissions = "0755"
    }
  }
}

resource "aws_efs_backup_policy" "mb46" {
  file_system_id = aws_efs_file_system.mb46.id

  backup_policy {
    status = "ENABLED"
  }
}

resource "aws_efs_file_system_policy" "mb46" {
  file_system_id = aws_efs_file_system.mb46.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch46"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "elasticfilesystem:ClientMount"
      Resource  = aws_efs_file_system.mb46.arn
    }]
  })
}

##############################################################################
# X-Ray: encryption config, a group, a resource-based policy, and a sampling
# rule.
##############################################################################

resource "aws_xray_encryption_config" "mb46" {
  type = "NONE"
}

resource "aws_xray_group" "mb46" {
  group_name        = "mega-batch-46-group"
  filter_expression = "responsetime > 5"
}

resource "aws_xray_resource_policy" "mb46" {
  policy_name     = "mega-batch-46-policy"
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch46XRay"
      Effect    = "Allow"
      Principal = { Service = "xray.amazonaws.com" }
      Action    = "xray:PutTraceSegments"
      Resource  = "*"
    }]
  })
}

resource "aws_xray_sampling_rule" "mb46" {
  rule_name      = "mega-batch-46-sampling"
  priority       = 1000
  version        = 1
  reservoir_size = 1
  fixed_rate     = 0.05
  url_path       = "*"
  host           = "*"
  http_method    = "*"
  service_type   = "*"
  service_name   = "*"
  resource_arn   = "*"
}

##############################################################################
# EventBridge: a resource-based bus policy, a cross-account permission, and a
# rule target pointed at an SQS queue.
##############################################################################

resource "aws_cloudwatch_event_bus" "mb46" {
  name = "mega-batch-46-bus"
}

resource "aws_cloudwatch_event_bus_policy" "mb46" {
  event_bus_name = aws_cloudwatch_event_bus.mb46.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "MegaBatch46Bus"
      Effect    = "Allow"
      Principal = { AWS = "999999999999" }
      Action    = "events:PutEvents"
      Resource  = aws_cloudwatch_event_bus.mb46.arn
    }]
  })
}

resource "aws_cloudwatch_event_bus" "mb46_perm" {
  name = "mega-batch-46-perm-bus"
}

resource "aws_cloudwatch_event_permission" "mb46" {
  principal      = "888888888888"
  statement_id   = "MegaBatch46Permission"
  event_bus_name = aws_cloudwatch_event_bus.mb46_perm.name
}

resource "aws_sqs_queue" "mb46_target" {
  name = "mega-batch-46-target-queue"
}

resource "aws_cloudwatch_event_rule" "mb46" {
  name           = "mega-batch-46-rule"
  event_bus_name = aws_cloudwatch_event_bus.mb46.name

  event_pattern = jsonencode({
    source = ["mega.batch.46"]
  })
}

resource "aws_cloudwatch_event_target" "mb46" {
  rule           = aws_cloudwatch_event_rule.mb46.name
  event_bus_name = aws_cloudwatch_event_bus.mb46.name
  arn            = aws_sqs_queue.mb46_target.arn
  target_id      = "mega-batch-46-target"
}

##############################################################################
# ElastiCache: a Redis user + user group, a second user associated to that
# group via a standalone association resource, and a serverless cache.
##############################################################################

resource "aws_elasticache_user" "mb46" {
  user_id       = "mega-batch-46-user"
  user_name     = "mega46"
  access_string = "on ~* +@all"
  engine        = "REDIS"

  authentication_mode {
    type = "no-password-required"
  }
}

resource "aws_elasticache_user_group" "mb46" {
  engine        = "REDIS"
  user_group_id = "mega-batch-46-ug"
  user_ids      = [aws_elasticache_user.mb46.user_id]
}

resource "aws_elasticache_user" "mb46_extra" {
  user_id       = "mega-batch-46-user-extra"
  user_name     = "mega46extra"
  access_string = "on ~* +@all"
  engine        = "REDIS"

  authentication_mode {
    type = "no-password-required"
  }
}

resource "aws_elasticache_user_group_association" "mb46" {
  user_group_id = aws_elasticache_user_group.mb46.user_group_id
  user_id       = aws_elasticache_user.mb46_extra.user_id
}

resource "aws_elasticache_serverless_cache" "mb46" {
  engine = "valkey"
  name   = "mega-batch-46-serverless"
}

##############################################################################
# DocumentDB: cluster + cluster parameter group, a cluster snapshot, an event
# subscription, and a standalone global cluster.
##############################################################################

resource "aws_docdb_subnet_group" "mb46" {
  name       = "mega-batch-46-docdb-sg"
  subnet_ids = ["subnet-mb46a", "subnet-mb46b"]
}

resource "aws_docdb_cluster_parameter_group" "mb46" {
  name        = "mega-batch-46-docdb-cpg"
  family      = "docdb5.0"
  description = "mega-batch-46 docdb cluster parameter group"
}

resource "aws_docdb_cluster" "mb46" {
  cluster_identifier              = "mega-batch-46-docdb"
  engine                          = "docdb"
  master_username                 = "admin"
  master_password                 = "megabatch46pw"
  db_subnet_group_name            = aws_docdb_subnet_group.mb46.name
  db_cluster_parameter_group_name = aws_docdb_cluster_parameter_group.mb46.name
  skip_final_snapshot             = true
}

resource "aws_docdb_cluster_snapshot" "mb46" {
  db_cluster_identifier          = aws_docdb_cluster.mb46.id
  db_cluster_snapshot_identifier = "mega-batch-46-docdb-snapshot"
}

resource "aws_sns_topic" "mb46_events" {
  name = "mega-batch-46-events-topic"
}

resource "aws_docdb_event_subscription" "mb46" {
  name          = "mega-batch-46-docdb-sub"
  sns_topic_arn = aws_sns_topic.mb46_events.arn

  source_type = "db-cluster"
  source_ids  = [aws_docdb_cluster.mb46.cluster_identifier]

  event_categories = ["maintenance"]
}

resource "aws_docdb_global_cluster" "mb46" {
  global_cluster_identifier = "mega-batch-46-docdb-global"
  engine                    = "docdb"
  engine_version            = "5.0.0"
}

##############################################################################
# Cost Explorer: an anomaly monitor + subscription, and a cost allocation tag.
##############################################################################

resource "aws_ce_anomaly_monitor" "mb46" {
  name              = "mega-batch-46-monitor"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}

resource "aws_sns_topic" "mb46_anomaly" {
  name = "mega-batch-46-anomaly-topic"

  policy = jsonencode({
    Version = "2008-10-17"
    Statement = [{
      Sid       = "MegaBatch46AllowCE"
      Effect    = "Allow"
      Principal = { Service = "costalerts.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = "arn:aws:sns:us-east-1:000000000000:mega-batch-46-anomaly-topic"
    }]
  })
}

resource "aws_ce_anomaly_subscription" "mb46" {
  name      = "mega-batch-46-subscription"
  frequency = "DAILY"

  monitor_arn_list = [aws_ce_anomaly_monitor.mb46.arn]

  subscriber {
    type    = "SNS"
    address = aws_sns_topic.mb46_anomaly.arn
  }

  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_ABSOLUTE"
      values        = ["100"]
      match_options = ["GREATER_THAN_OR_EQUAL"]
    }
  }
}

resource "aws_ce_cost_allocation_tag" "mb46" {
  tag_key = "mega-batch-46-tag"
  status  = "Active"
}

##############################################################################
# CodeCommit: a repository, an approval rule template + association, and a
# trigger.
##############################################################################

resource "aws_codecommit_repository" "mb46" {
  repository_name = "mega-batch-46-repo"
}

resource "aws_codecommit_approval_rule_template" "mb46" {
  name    = "mega-batch-46-approval-template"
  content = jsonencode({
    Version               = "2018-11-08"
    DestinationReferences = ["refs/heads/main"]
    Statements = [{
      Type                    = "Approvers"
      NumberOfApprovalsNeeded = 1
    }]
  })
}

resource "aws_codecommit_approval_rule_template_association" "mb46" {
  approval_rule_template_name = aws_codecommit_approval_rule_template.mb46.name
  repository_name             = aws_codecommit_repository.mb46.repository_name
}

resource "aws_sns_topic" "mb46_trigger" {
  name = "mega-batch-46-trigger-topic"
}

resource "aws_codecommit_trigger" "mb46" {
  repository_name = aws_codecommit_repository.mb46.repository_name

  trigger {
    name            = "mega-batch-46-trigger"
    destination_arn = aws_sns_topic.mb46_trigger.arn
    events          = ["all"]
  }
}

##############################################################################
# Amplify: an app, a backend environment, a domain association, and a
# webhook.
##############################################################################

resource "aws_amplify_app" "mb46" {
  name = "mega-batch-46-app"
}

resource "aws_amplify_branch" "mb46" {
  app_id      = aws_amplify_app.mb46.id
  branch_name = "main"
}

resource "aws_amplify_backend_environment" "mb46" {
  app_id           = aws_amplify_app.mb46.id
  environment_name = "mbfortysix"
}

resource "aws_amplify_domain_association" "mb46" {
  app_id      = aws_amplify_app.mb46.id
  domain_name = "mega-batch-46.example.test"

  sub_domain {
    branch_name = aws_amplify_branch.mb46.branch_name
    prefix      = ""
  }
}

resource "aws_amplify_webhook" "mb46" {
  app_id      = aws_amplify_app.mb46.id
  branch_name = aws_amplify_branch.mb46.branch_name
  description = "mega-batch-46 webhook"
}
