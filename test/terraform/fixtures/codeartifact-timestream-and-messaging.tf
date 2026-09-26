##############################################################################
# Standalone singleton/simple resources: Batch scheduling policy, DAX
# parameter group, IAM STS preferences, Scheduler schedule group, Step
# Functions activity, CodeDeploy deployment config, and Timestream tables.
##############################################################################

resource "aws_batch_scheduling_policy" "ctmm" {
  name = "ctmm-sched-policy"

  fair_share_policy {
    compute_reservation = 1
    share_decay_seconds = 3600

    share_distribution {
      share_identifier = "A1*"
      weight_factor    = 0.1
    }
  }
}

resource "aws_dax_parameter_group" "ctmm" {
  name = "ctmm-dax-pg"

  # Both of DAX's two tunable parameters always carry a value (default or
  # user-set) and both come back from DescribeParameters regardless -- so
  # both must be declared here or the un-declared one drifts every plan.
  parameters {
    name  = "query-ttl-millis"
    value = "100000"
  }

  parameters {
    name  = "record-ttl-millis"
    value = "300000"
  }
}

resource "aws_iam_security_token_service_preferences" "ctmm" {
  global_endpoint_token_version = "v2Token"
}

resource "aws_scheduler_schedule_group" "ctmm" {
  name = "ctmm-sched-group"
}

resource "aws_sfn_activity" "ctmm" {
  name = "ctmm-activity"
}

resource "aws_codedeploy_deployment_config" "ctmm" {
  deployment_config_name = "ctmm-deploy-config"

  minimum_healthy_hosts {
    type  = "HOST_COUNT"
    value = 1
  }
}

resource "aws_mq_configuration" "ctmm" {
  name           = "ctmm-mq-config"
  engine_type    = "ACTIVEMQ"
  engine_version = "5.17.6"
  data           = <<DATA
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<broker xmlns="http://activemq.apache.org/schema/core">
</broker>
DATA
}

resource "aws_timestreamwrite_database" "ctmm" {
  database_name = "ctmm-timestream-db"
}

resource "aws_timestreamwrite_table" "ctmm" {
  database_name = aws_timestreamwrite_database.ctmm.database_name
  table_name    = "ctmm-timestream-table"
}

##############################################################################
# DAX subnet group (needs its own VPC/subnets).
##############################################################################

resource "aws_vpc" "ctmm" {
  cidr_block = "10.210.0.0/16"
}

resource "aws_subnet" "ctmm_a" {
  vpc_id            = aws_vpc.ctmm.id
  cidr_block        = "10.210.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "ctmm_b" {
  vpc_id            = aws_vpc.ctmm.id
  cidr_block        = "10.210.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_dax_subnet_group" "ctmm" {
  name       = "ctmm-dax-sg"
  subnet_ids = [aws_subnet.ctmm_a.id, aws_subnet.ctmm_b.id]
}

##############################################################################
# MemoryDB parameter group + user.
##############################################################################

resource "aws_memorydb_parameter_group" "ctmm" {
  name   = "ctmm-memorydb-pg"
  family = "memorydb_redis7"
}

resource "aws_memorydb_user" "ctmm" {
  user_name     = "ctmm-memorydb-user"
  access_string = "on ~* &* +@all"

  authentication_mode {
    type      = "password"
    passwords = ["CtmmPassword1234567890"]
  }
}

##############################################################################
# Secrets Manager secret + resource policy.
##############################################################################

resource "aws_secretsmanager_secret" "ctmm" {
  name = "ctmm-secret"
}

resource "aws_secretsmanager_secret_policy" "ctmm" {
  secret_arn = aws_secretsmanager_secret.ctmm.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" }
      Action    = "secretsmanager:GetSecretValue"
      Resource  = "*"
    }]
  })
}

##############################################################################
# SQS queue + policy; SNS topic + policy; ResourceGroups group + membership.
##############################################################################

resource "aws_sqs_queue" "ctmm" {
  name = "ctmm-queue"
}

resource "aws_sqs_queue_policy" "ctmm" {
  queue_url = aws_sqs_queue.ctmm.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ctmm"
      Effect    = "Allow"
      Principal = "*"
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.ctmm.arn
    }]
  })
}

resource "aws_sns_topic" "ctmm" {
  name = "ctmm-topic"
}

resource "aws_sns_topic_policy" "ctmm" {
  arn = aws_sns_topic.ctmm.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ctmm"
      Effect    = "Allow"
      Principal = "*"
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.ctmm.arn
    }]
  })
}

resource "aws_resourcegroups_group" "ctmm" {
  name = "ctmm-rg"

  # A statically-membered group (Configuration, not ResourceQuery): membership
  # for a TAG_FILTERS/CLOUDFORMATION_STACK query group is computed dynamically
  # from the query and rejects explicit GroupResources/UngroupResources calls.
  configuration {
    type = "AWS::ResourceGroups::Generic"
  }
}

resource "aws_resourcegroups_resource" "ctmm" {
  group_arn    = aws_resourcegroups_group.ctmm.arn
  resource_arn = aws_sqs_queue.ctmm.arn
}

##############################################################################
# CodeArtifact domain + repository permissions policies.
##############################################################################

resource "aws_codeartifact_domain" "ctmm" {
  domain = "ctmm-domain"
}

resource "aws_codeartifact_domain_permissions_policy" "ctmm" {
  domain = aws_codeartifact_domain.ctmm.domain
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "codeartifact:CreateRepository"
      Resource  = aws_codeartifact_domain.ctmm.arn
    }]
  })
}

resource "aws_codeartifact_repository" "ctmm" {
  repository = "ctmm-repo"
  domain     = aws_codeartifact_domain.ctmm.domain
}

resource "aws_codeartifact_repository_permissions_policy" "ctmm" {
  domain     = aws_codeartifact_domain.ctmm.domain
  repository = aws_codeartifact_repository.ctmm.repository
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "codeartifact:ReadFromRepository"
      Resource  = aws_codeartifact_repository.ctmm.arn
    }]
  })
}

##############################################################################
# CloudTrail Lake event data store.
##############################################################################

resource "aws_cloudtrail_event_data_store" "ctmm" {
  name                           = "ctmm-eds"
  termination_protection_enabled = false

  advanced_event_selector {
    name = "ctmm-management-events"

    field_selector {
      field  = "eventCategory"
      equals = ["Management"]
    }
  }
}

