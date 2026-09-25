##############################################################################
# EFS: file system with an access point, a backup policy, and a
# resource-based file system policy.
##############################################################################

resource "aws_efs_file_system" "dbae" {
  tags = {
    Name = "dbae-efs"
  }
}

resource "aws_efs_access_point" "dbae" {
  file_system_id = aws_efs_file_system.dbae.id

  posix_user {
    gid = 1000
    uid = 1000
  }

  root_directory {
    path = "/dbae"

    creation_info {
      owner_gid   = 1000
      owner_uid   = 1000
      permissions = "0755"
    }
  }
}

resource "aws_efs_backup_policy" "dbae" {
  file_system_id = aws_efs_file_system.dbae.id

  backup_policy {
    status = "ENABLED"
  }
}

resource "aws_efs_file_system_policy" "dbae" {
  file_system_id = aws_efs_file_system.dbae.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "Dbae"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "elasticfilesystem:ClientMount"
      Resource  = aws_efs_file_system.dbae.arn
    }]
  })
}

##############################################################################
# X-Ray: encryption config, a group, a resource-based policy, and a sampling
# rule.
##############################################################################

resource "aws_xray_encryption_config" "dbae" {
  type = "NONE"
}

resource "aws_xray_group" "dbae" {
  group_name        = "dbae-group"
  filter_expression = "responsetime > 5"
}

resource "aws_xray_resource_policy" "dbae" {
  policy_name = "dbae-policy"
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DbaeXRay"
      Effect    = "Allow"
      Principal = { Service = "xray.amazonaws.com" }
      Action    = "xray:PutTraceSegments"
      Resource  = "*"
    }]
  })
}

resource "aws_xray_sampling_rule" "dbae" {
  rule_name      = "dbae-sampling"
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

resource "aws_cloudwatch_event_bus" "dbae" {
  name = "dbae-bus"
}

resource "aws_cloudwatch_event_bus_policy" "dbae" {
  event_bus_name = aws_cloudwatch_event_bus.dbae.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DbaeBus"
      Effect    = "Allow"
      Principal = { AWS = "999999999999" }
      Action    = "events:PutEvents"
      Resource  = aws_cloudwatch_event_bus.dbae.arn
    }]
  })
}

resource "aws_cloudwatch_event_bus" "dbae_perm" {
  name = "dbae-perm-bus"
}

resource "aws_cloudwatch_event_permission" "dbae" {
  principal      = "888888888888"
  statement_id   = "DbaePermission"
  event_bus_name = aws_cloudwatch_event_bus.dbae_perm.name
}

resource "aws_sqs_queue" "dbae_target" {
  name = "dbae-target-queue"
}

resource "aws_cloudwatch_event_rule" "dbae" {
  name           = "dbae-rule"
  event_bus_name = aws_cloudwatch_event_bus.dbae.name

  event_pattern = jsonencode({
    source = ["dbae"]
  })
}

resource "aws_cloudwatch_event_target" "dbae" {
  rule           = aws_cloudwatch_event_rule.dbae.name
  event_bus_name = aws_cloudwatch_event_bus.dbae.name
  arn            = aws_sqs_queue.dbae_target.arn
  target_id      = "dbae-target"
}

##############################################################################
# ElastiCache: a Redis user + user group, a second user associated to that
# group via a standalone association resource, and a serverless cache.
##############################################################################

resource "aws_elasticache_user" "dbae" {
  user_id       = "dbae-user"
  user_name     = "mega46"
  access_string = "on ~* +@all"
  engine        = "REDIS"

  authentication_mode {
    type = "no-password-required"
  }
}

resource "aws_elasticache_user_group" "dbae" {
  engine        = "REDIS"
  user_group_id = "dbae-ug"
  user_ids      = [aws_elasticache_user.dbae.user_id]
}

resource "aws_elasticache_user" "dbae_extra" {
  user_id       = "dbae-user-extra"
  user_name     = "mega46extra"
  access_string = "on ~* +@all"
  engine        = "REDIS"

  authentication_mode {
    type = "no-password-required"
  }
}

resource "aws_elasticache_user_group_association" "dbae" {
  user_group_id = aws_elasticache_user_group.dbae.user_group_id
  user_id       = aws_elasticache_user.dbae_extra.user_id
}

resource "aws_elasticache_serverless_cache" "dbae" {
  engine = "valkey"
  name   = "dbae-serverless"
}

##############################################################################
# DocumentDB: cluster + cluster parameter group, a cluster snapshot, an event
# subscription, and a standalone global cluster.
##############################################################################

resource "aws_docdb_subnet_group" "dbae" {
  name       = "dbae-docdb-sg"
  subnet_ids = ["subnet-dbaea", "subnet-dbaeb"]
}

resource "aws_docdb_cluster_parameter_group" "dbae" {
  name        = "dbae-docdb-cpg"
  family      = "docdb5.0"
  description = "dbae docdb cluster parameter group"
}

resource "aws_docdb_cluster" "dbae" {
  cluster_identifier              = "dbae-docdb"
  engine                          = "docdb"
  master_username                 = "admin"
  master_password                 = "dbaepassword1"
  db_subnet_group_name            = aws_docdb_subnet_group.dbae.name
  db_cluster_parameter_group_name = aws_docdb_cluster_parameter_group.dbae.name
  skip_final_snapshot             = true
}

resource "aws_docdb_cluster_snapshot" "dbae" {
  db_cluster_identifier          = aws_docdb_cluster.dbae.id
  db_cluster_snapshot_identifier = "dbae-docdb-snapshot"
}

resource "aws_sns_topic" "dbae_events" {
  name = "dbae-events-topic"
}

resource "aws_docdb_event_subscription" "dbae" {
  name          = "dbae-docdb-sub"
  sns_topic_arn = aws_sns_topic.dbae_events.arn

  source_type = "db-cluster"
  source_ids  = [aws_docdb_cluster.dbae.cluster_identifier]

  event_categories = ["maintenance"]
}

resource "aws_docdb_global_cluster" "dbae" {
  global_cluster_identifier = "dbae-docdb-global"
  engine                    = "docdb"
  engine_version            = "5.0.0"
}

##############################################################################
# Cost Explorer: an anomaly monitor + subscription, and a cost allocation tag.
##############################################################################

resource "aws_ce_anomaly_monitor" "dbae" {
  name              = "dbae-monitor"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}

resource "aws_sns_topic" "dbae_anomaly" {
  name = "dbae-anomaly-topic"

  policy = jsonencode({
    Version = "2008-10-17"
    Statement = [{
      Sid       = "DbaeAllowCE"
      Effect    = "Allow"
      Principal = { Service = "costalerts.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = "arn:aws:sns:us-east-1:000000000000:dbae-anomaly-topic"
    }]
  })
}

resource "aws_ce_anomaly_subscription" "dbae" {
  name      = "dbae-subscription"
  frequency = "DAILY"

  monitor_arn_list = [aws_ce_anomaly_monitor.dbae.arn]

  subscriber {
    type    = "SNS"
    address = aws_sns_topic.dbae_anomaly.arn
  }

  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_ABSOLUTE"
      values        = ["100"]
      match_options = ["GREATER_THAN_OR_EQUAL"]
    }
  }
}

resource "aws_ce_cost_allocation_tag" "dbae" {
  tag_key = "dbae-tag"
  status  = "Active"
}

##############################################################################
# CodeCommit: a repository, an approval rule template + association, and a
# trigger.
##############################################################################

resource "aws_codecommit_repository" "dbae" {
  repository_name = "dbae-repo"
}

resource "aws_codecommit_approval_rule_template" "dbae" {
  name = "dbae-approval-template"
  content = jsonencode({
    Version               = "2018-11-08"
    DestinationReferences = ["refs/heads/main"]
    Statements = [{
      Type                    = "Approvers"
      NumberOfApprovalsNeeded = 1
    }]
  })
}

resource "aws_codecommit_approval_rule_template_association" "dbae" {
  approval_rule_template_name = aws_codecommit_approval_rule_template.dbae.name
  repository_name             = aws_codecommit_repository.dbae.repository_name
}

resource "aws_sns_topic" "dbae_trigger" {
  name = "dbae-trigger-topic"
}

resource "aws_codecommit_trigger" "dbae" {
  repository_name = aws_codecommit_repository.dbae.repository_name

  trigger {
    name            = "dbae-trigger"
    destination_arn = aws_sns_topic.dbae_trigger.arn
    events          = ["all"]
  }
}

##############################################################################
# Amplify: an app, a backend environment, a domain association, and a
# webhook.
##############################################################################

resource "aws_amplify_app" "dbae" {
  name = "dbae-app"
}

resource "aws_amplify_branch" "dbae" {
  app_id      = aws_amplify_app.dbae.id
  branch_name = "main"
}

resource "aws_amplify_backend_environment" "dbae" {
  app_id           = aws_amplify_app.dbae.id
  environment_name = "mbfortysix"
}

resource "aws_amplify_domain_association" "dbae" {
  app_id      = aws_amplify_app.dbae.id
  domain_name = "dbae.example.test"

  sub_domain {
    branch_name = aws_amplify_branch.dbae.branch_name
    prefix      = ""
  }
}

resource "aws_amplify_webhook" "dbae" {
  app_id      = aws_amplify_app.dbae.id
  branch_name = aws_amplify_branch.dbae.branch_name
  description = "dbae webhook"
}
