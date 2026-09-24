##############################################################################
# Application Auto Scaling: policy + scheduled action against a DynamoDB
# scalable target.
##############################################################################

resource "aws_dynamodb_table" "mb52" {
  name         = "mega-batch-52-ddb"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}

resource "aws_appautoscaling_target" "mb52" {
  service_namespace  = "dynamodb"
  resource_id        = "table/${aws_dynamodb_table.mb52.name}"
  scalable_dimension = "dynamodb:table:ReadCapacityUnits"
  min_capacity       = 1
  max_capacity       = 10
}

resource "aws_appautoscaling_policy" "mb52" {
  name               = "mega-batch-52-scaling-policy"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.mb52.service_namespace
  resource_id        = aws_appautoscaling_target.mb52.resource_id
  scalable_dimension = aws_appautoscaling_target.mb52.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value = 50.0

    predefined_metric_specification {
      predefined_metric_type = "DynamoDBReadCapacityUtilization"
    }
  }
}

resource "aws_appautoscaling_scheduled_action" "mb52" {
  name               = "mega-batch-52-scheduled-action"
  service_namespace  = aws_appautoscaling_target.mb52.service_namespace
  resource_id        = aws_appautoscaling_target.mb52.resource_id
  scalable_dimension = aws_appautoscaling_target.mb52.scalable_dimension
  schedule           = "at(2030-01-01T00:00:00)"

  scalable_target_action {
    min_capacity = 1
    max_capacity = 5
  }
}

##############################################################################
# Standalone singleton/simple resources: Bedrock Agents prompt, ECR registry
# scanning config, Glacier vault lock, RolesAnywhere profile, Rekognition
# project, QuickSight account subscription + settings.
##############################################################################

resource "aws_bedrockagent_prompt" "mb52" {
  name        = "mega-batch-52-prompt"
  description = "mega-batch-52 test prompt"
}

resource "aws_ecr_registry_scanning_configuration" "mb52" {
  scan_type = "BASIC"

  rule {
    scan_frequency = "SCAN_ON_PUSH"

    repository_filter {
      filter      = "*"
      filter_type = "WILDCARD"
    }
  }
}

resource "aws_glacier_vault" "mb52" {
  name = "mega-batch-52-vault"
}

resource "aws_glacier_vault_lock" "mb52" {
  vault_name    = aws_glacier_vault.mb52.name
  complete_lock = false
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb52"
      Effect    = "Deny"
      Principal = { AWS = "*" }
      Action    = "glacier:DeleteArchive"
      Resource  = aws_glacier_vault.mb52.arn
    }]
  })
}

resource "aws_iam_role" "mb52_rolesanywhere" {
  name = "mega-batch-52-rolesanywhere-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "rolesanywhere.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]
    }]
  })
}

resource "aws_rolesanywhere_profile" "mb52" {
  name      = "mega-batch-52-profile"
  role_arns = [aws_iam_role.mb52_rolesanywhere.arn]
}

resource "aws_rekognition_project" "mb52" {
  name    = "mega-batch-52-project"
  feature = "CUSTOM_LABELS"
}

resource "aws_quicksight_account_subscription" "mb52" {
  account_name          = "mega-batch-52-qs"
  authentication_method = "IAM_AND_QUICKSIGHT"
  edition               = "ENTERPRISE"
  notification_email    = "mb52@example.com"
}

resource "aws_quicksight_account_settings" "mb52" {
  termination_protection_enabled = false

  depends_on = [aws_quicksight_account_subscription.mb52]
}

##############################################################################
# Kinesis: stream + resource policy + consumer.
##############################################################################

resource "aws_kinesis_stream" "mb52" {
  name        = "mega-batch-52-stream"
  shard_count = 1
}

resource "aws_kinesis_resource_policy" "mb52" {
  resource_arn = aws_kinesis_stream.mb52.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb52"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "kinesis:GetRecords"
      Resource  = aws_kinesis_stream.mb52.arn
    }]
  })
}

resource "aws_kinesis_stream_consumer" "mb52" {
  name       = "mega-batch-52-consumer"
  stream_arn = aws_kinesis_stream.mb52.arn
}

##############################################################################
# Lake Formation: LF-tag + resource LF-tag attachment to a Glue database.
##############################################################################

resource "aws_glue_catalog_database" "mb52" {
  name = "mega_batch_52_db"
}

resource "aws_lakeformation_lf_tag" "mb52" {
  key    = "mega-batch-52-tag"
  values = ["blue", "green"]
}

resource "aws_lakeformation_resource_lf_tag" "mb52" {
  database {
    name = aws_glue_catalog_database.mb52.name
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.mb52.key
    value = "blue"
  }
}

##############################################################################
# MediaStore container + policy.
##############################################################################

resource "aws_media_store_container" "mb52" {
  name = "mega_batch_52_container"
}

resource "aws_media_store_container_policy" "mb52" {
  container_name = aws_media_store_container.mb52.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb52"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "mediastore:GetObject"
      Resource  = "${aws_media_store_container.mb52.arn}/*"
    }]
  })
}

##############################################################################
# S3 Tables: table bucket + namespace + table, and both policy resources.
##############################################################################

resource "aws_s3tables_table_bucket" "mb52" {
  name = "mega-batch-52-tb"
}

resource "aws_s3tables_table_bucket_policy" "mb52" {
  table_bucket_arn = aws_s3tables_table_bucket.mb52.arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb52"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "s3tables:GetTableBucket"
      Resource  = aws_s3tables_table_bucket.mb52.arn
    }]
  })
}

resource "aws_s3tables_namespace" "mb52" {
  namespace        = "mega_batch_52_ns"
  table_bucket_arn = aws_s3tables_table_bucket.mb52.arn
}

resource "aws_s3tables_table" "mb52" {
  name             = "mega_batch_52_table"
  namespace        = aws_s3tables_namespace.mb52.namespace
  table_bucket_arn = aws_s3tables_table_bucket.mb52.arn
  format           = "ICEBERG"
}

resource "aws_s3tables_table_policy" "mb52" {
  name             = aws_s3tables_table.mb52.name
  namespace        = aws_s3tables_namespace.mb52.namespace
  table_bucket_arn = aws_s3tables_table_bucket.mb52.arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "mb52"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "s3tables:GetTable"
      Resource  = aws_s3tables_table.mb52.arn
    }]
  })
}

##############################################################################
# Service Discovery: public + private DNS namespaces (own VPC).
##############################################################################

resource "aws_vpc" "mb52" {
  cidr_block = "10.211.0.0/16"
}

resource "aws_subnet" "mb52" {
  vpc_id            = aws_vpc.mb52.id
  cidr_block        = "10.211.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_service_discovery_public_dns_namespace" "mb52" {
  name = "mega-batch-52.example.com"
}

resource "aws_service_discovery_private_dns_namespace" "mb52" {
  name = "mega-batch-52.private"
  vpc  = aws_vpc.mb52.id
}

##############################################################################
# SNS: SMS preferences (account singleton) + topic data protection policy.
##############################################################################

resource "aws_sns_topic" "mb52" {
  name = "mega-batch-52-topic"
}

resource "aws_sns_sms_preferences" "mb52" {
  default_sms_type = "Transactional"
}

resource "aws_sns_topic_data_protection_policy" "mb52" {
  arn = aws_sns_topic.mb52.arn
  policy = jsonencode({
    Name      = "mega-batch-52-dpp"
    Version   = "2021-06-01"
    Statement = []
  })
}

##############################################################################
# SQS: separate redrive-policy attachment resource against a DLQ.
##############################################################################

resource "aws_sqs_queue" "mb52_dlq" {
  name = "mega-batch-52-dlq"
}

resource "aws_sqs_queue" "mb52_src" {
  name = "mega-batch-52-src"
}

resource "aws_sqs_queue_redrive_policy" "mb52" {
  queue_url = aws_sqs_queue.mb52_src.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.mb52_dlq.arn
    maxReceiveCount     = 5
  })
}

##############################################################################
# Transcribe: vocabulary filter.
##############################################################################

resource "aws_transcribe_vocabulary_filter" "mb52" {
  vocabulary_filter_name = "mega-batch-52-filter"
  language_code          = "en-US"
  words                  = ["foo", "bar"]
}

##############################################################################
# CodePipeline: custom action type.
##############################################################################

resource "aws_codepipeline_custom_action_type" "mb52" {
  category      = "Build"
  provider_name = "mega-batch-52-provider"
  version       = "1"

  input_artifact_details {
    maximum_count = 1
    minimum_count = 0
  }

  output_artifact_details {
    maximum_count = 1
    minimum_count = 0
  }
}

##############################################################################
# API Gateway: PutRestApi import against an existing REST API.
##############################################################################

resource "aws_api_gateway_rest_api" "mb52" {
  name = "mega-batch-52-api"
}

resource "aws_api_gateway_rest_api_put" "mb52" {
  rest_api_id      = aws_api_gateway_rest_api.mb52.id
  fail_on_warnings = false

  parameters = {
    mode = "merge"
  }

  body = jsonencode({
    openapi = "3.0.1"
    info = {
      title   = "mega-batch-52-api"
      version = "1.0"
    }
    paths = {
      "/mb52" = {
        get = {
          responses = {
            "200" = { description = "ok" }
          }
        }
      }
    }
  })
}

##############################################################################
# Classic ELB attachment against a real EC2 instance.
##############################################################################

resource "aws_vpc" "mb52_elb" {
  cidr_block = "10.212.0.0/16"
}

resource "aws_subnet" "mb52_elb" {
  vpc_id            = aws_vpc.mb52_elb.id
  cidr_block        = "10.212.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_instance" "mb52" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.mb52_elb.id

  tags = {
    Name = "mega-batch-52-instance"
  }
}

resource "aws_elb" "mb52" {
  name               = "mega-batch-52-lb"
  availability_zones = ["us-east-1a"]

  listener {
    instance_port     = 80
    instance_protocol = "HTTP"
    lb_port           = 80
    lb_protocol       = "HTTP"
  }
}

resource "aws_elb_attachment" "mb52" {
  elb      = aws_elb.mb52.id
  instance = aws_instance.mb52.id
}
