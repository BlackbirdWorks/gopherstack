##############################################################################
# Standalone singleton/simple resources: Batch scheduling policy, DAX
# parameter group, IAM STS preferences, Scheduler schedule group, Step
# Functions activity, CodeDeploy deployment config, and Timestream tables.
##############################################################################

resource "aws_batch_scheduling_policy" "mb51" {
  name = "mega-batch-51-sched-policy"

  fair_share_policy {
    compute_reservation = 1
    share_decay_seconds = 3600

    share_distribution {
      share_identifier = "A1*"
      weight_factor    = 0.1
    }
  }
}

resource "aws_dax_parameter_group" "mb51" {
  name = "mega-batch-51-dax-pg"

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

resource "aws_iam_security_token_service_preferences" "mb51" {
  global_endpoint_token_version = "v2Token"
}

resource "aws_scheduler_schedule_group" "mb51" {
  name = "mega-batch-51-sched-group"
}

resource "aws_sfn_activity" "mb51" {
  name = "mega-batch-51-activity"
}

resource "aws_codedeploy_deployment_config" "mb51" {
  deployment_config_name = "mega-batch-51-deploy-config"

  minimum_healthy_hosts {
    type  = "HOST_COUNT"
    value = 1
  }
}

resource "aws_mq_configuration" "mb51" {
  name           = "mega-batch-51-mq-config"
  engine_type    = "ACTIVEMQ"
  engine_version = "5.17.6"
  data           = <<DATA
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<broker xmlns="http://activemq.apache.org/schema/core">
</broker>
DATA
}

resource "aws_timestreamwrite_database" "mb51" {
  database_name = "mega-batch-51-timestream-db"
}

resource "aws_timestreamwrite_table" "mb51" {
  database_name = aws_timestreamwrite_database.mb51.database_name
  table_name    = "mega-batch-51-timestream-table"
}

##############################################################################
# DAX subnet group (needs its own VPC/subnets).
##############################################################################

resource "aws_vpc" "mb51" {
  cidr_block = "10.210.0.0/16"
}

resource "aws_subnet" "mb51_a" {
  vpc_id            = aws_vpc.mb51.id
  cidr_block        = "10.210.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "mb51_b" {
  vpc_id            = aws_vpc.mb51.id
  cidr_block        = "10.210.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_dax_subnet_group" "mb51" {
  name       = "mega-batch-51-dax-sg"
  subnet_ids = [aws_subnet.mb51_a.id, aws_subnet.mb51_b.id]
}

##############################################################################
# MemoryDB parameter group + user.
##############################################################################

resource "aws_memorydb_parameter_group" "mb51" {
  name   = "mega-batch-51-memorydb-pg"
  family = "memorydb_redis7"
}

resource "aws_memorydb_user" "mb51" {
  user_name     = "mega-batch-51-memorydb-user"
  access_string = "on ~* &* +@all"

  authentication_mode {
    type      = "password"
    passwords = ["MegaBatch51Password1234567890"]
  }
}

##############################################################################
# Secrets Manager secret + resource policy.
##############################################################################

resource "aws_secretsmanager_secret" "mb51" {
  name = "mega-batch-51-secret"
}

resource "aws_secretsmanager_secret_policy" "mb51" {
  secret_arn = aws_secretsmanager_secret.mb51.arn
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

resource "aws_sqs_queue" "mb51" {
  name = "mega-batch-51-queue"
}

resource "aws_sqs_queue_policy" "mb51" {
  queue_url = aws_sqs_queue.mb51.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb51"
      Effect    = "Allow"
      Principal = "*"
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.mb51.arn
    }]
  })
}

resource "aws_sns_topic" "mb51" {
  name = "mega-batch-51-topic"
}

resource "aws_sns_topic_policy" "mb51" {
  arn = aws_sns_topic.mb51.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb51"
      Effect    = "Allow"
      Principal = "*"
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.mb51.arn
    }]
  })
}

resource "aws_resourcegroups_group" "mb51" {
  name = "mega-batch-51-rg"

  # A statically-membered group (Configuration, not ResourceQuery): membership
  # for a TAG_FILTERS/CLOUDFORMATION_STACK query group is computed dynamically
  # from the query and rejects explicit GroupResources/UngroupResources calls.
  configuration {
    type = "AWS::ResourceGroups::Generic"
  }
}

resource "aws_resourcegroups_resource" "mb51" {
  group_arn    = aws_resourcegroups_group.mb51.arn
  resource_arn = aws_sqs_queue.mb51.arn
}

##############################################################################
# CodeArtifact domain + repository permissions policies.
##############################################################################

resource "aws_codeartifact_domain" "mb51" {
  domain = "mega-batch-51-domain"
}

resource "aws_codeartifact_domain_permissions_policy" "mb51" {
  domain = aws_codeartifact_domain.mb51.domain
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "codeartifact:CreateRepository"
      Resource  = aws_codeartifact_domain.mb51.arn
    }]
  })
}

resource "aws_codeartifact_repository" "mb51" {
  repository = "mega-batch-51-repo"
  domain     = aws_codeartifact_domain.mb51.domain
}

resource "aws_codeartifact_repository_permissions_policy" "mb51" {
  domain     = aws_codeartifact_domain.mb51.domain
  repository = aws_codeartifact_repository.mb51.repository
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "codeartifact:ReadFromRepository"
      Resource  = aws_codeartifact_repository.mb51.arn
    }]
  })
}

##############################################################################
# CloudTrail Lake event data store.
##############################################################################

resource "aws_cloudtrail_event_data_store" "mb51" {
  name                           = "mega-batch-51-eds"
  termination_protection_enabled = false

  advanced_event_selector {
    name = "mb51-management-events"

    field_selector {
      field  = "eventCategory"
      equals = ["Management"]
    }
  }
}

