##############################################################################
# Application Auto Scaling: policy + scheduled action against a DynamoDB
# scalable target.
##############################################################################

resource "aws_dynamodb_table" "s3ms" {
  name           = "s3ms-ddb"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "pk"

  attribute {
    name = "pk"
    type = "S"
  }
}

resource "aws_appautoscaling_target" "s3ms" {
  service_namespace  = "dynamodb"
  resource_id        = "table/${aws_dynamodb_table.s3ms.name}"
  scalable_dimension = "dynamodb:table:ReadCapacityUnits"
  min_capacity       = 1
  max_capacity       = 10
}

resource "aws_appautoscaling_policy" "s3ms" {
  name               = "s3ms-scaling-policy"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.s3ms.service_namespace
  resource_id        = aws_appautoscaling_target.s3ms.resource_id
  scalable_dimension = aws_appautoscaling_target.s3ms.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value = 50.0

    predefined_metric_specification {
      predefined_metric_type = "DynamoDBReadCapacityUtilization"
    }
  }
}

resource "aws_appautoscaling_scheduled_action" "s3ms" {
  name               = "s3ms-scheduled-action"
  service_namespace  = aws_appautoscaling_target.s3ms.service_namespace
  resource_id        = aws_appautoscaling_target.s3ms.resource_id
  scalable_dimension = aws_appautoscaling_target.s3ms.scalable_dimension
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

resource "aws_bedrockagent_prompt" "s3ms" {
  name        = "s3ms-prompt"
  description = "s3ms test prompt"
}

resource "aws_ecr_registry_scanning_configuration" "s3ms" {
  scan_type = "BASIC"

  rule {
    scan_frequency = "SCAN_ON_PUSH"

    repository_filter {
      filter      = "*"
      filter_type = "WILDCARD"
    }
  }
}

resource "aws_glacier_vault" "s3ms" {
  name = "s3ms-vault"
}

resource "aws_glacier_vault_lock" "s3ms" {
  vault_name    = aws_glacier_vault.s3ms.name
  complete_lock = false
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "s3ms"
      Effect    = "Deny"
      Principal = { AWS = "*" }
      Action    = "glacier:DeleteArchive"
      Resource  = aws_glacier_vault.s3ms.arn
    }]
  })
}

resource "aws_iam_role" "s3ms_rolesanywhere" {
  name = "s3ms-rolesanywhere-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "rolesanywhere.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]
    }]
  })
}

resource "aws_rolesanywhere_profile" "s3ms" {
  name      = "s3ms-profile"
  role_arns = [aws_iam_role.s3ms_rolesanywhere.arn]
}

resource "aws_rekognition_project" "s3ms" {
  name    = "s3ms-project"
  feature = "CUSTOM_LABELS"
}

resource "aws_quicksight_account_subscription" "s3ms" {
  account_name          = "s3ms-qs"
  authentication_method = "IAM_AND_QUICKSIGHT"
  edition               = "ENTERPRISE"
  notification_email    = "s3ms@example.com"
}

resource "aws_quicksight_account_settings" "s3ms" {
  termination_protection_enabled = false

  depends_on = [aws_quicksight_account_subscription.s3ms]
}

##############################################################################
# Kinesis: stream + resource policy + consumer.
##############################################################################

resource "aws_kinesis_stream" "s3ms" {
  name        = "s3ms-stream"
  shard_count = 1
}

resource "aws_kinesis_resource_policy" "s3ms" {
  resource_arn = aws_kinesis_stream.s3ms.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "s3ms"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "kinesis:GetRecords"
      Resource  = aws_kinesis_stream.s3ms.arn
    }]
  })
}

resource "aws_kinesis_stream_consumer" "s3ms" {
  name       = "s3ms-consumer"
  stream_arn = aws_kinesis_stream.s3ms.arn
}

##############################################################################
# Lake Formation: LF-tag + resource LF-tag attachment to a Glue database.
##############################################################################

resource "aws_glue_catalog_database" "s3ms" {
  name = "s3ms_db"
}

resource "aws_lakeformation_lf_tag" "s3ms" {
  key    = "s3ms-tag"
  values = ["blue", "green"]
}

resource "aws_lakeformation_resource_lf_tag" "s3ms" {
  database {
    name = aws_glue_catalog_database.s3ms.name
  }

  lf_tag {
    key   = aws_lakeformation_lf_tag.s3ms.key
    value = "blue"
  }
}

##############################################################################
# MediaStore container + policy.
##############################################################################

resource "aws_media_store_container" "s3ms" {
  name = "s3ms_container"
}

resource "aws_media_store_container_policy" "s3ms" {
  container_name = aws_media_store_container.s3ms.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "s3ms"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "mediastore:GetObject"
      Resource  = "${aws_media_store_container.s3ms.arn}/*"
    }]
  })
}

##############################################################################
# S3 Tables: table bucket + namespace + table, and both policy resources.
##############################################################################

resource "aws_s3tables_table_bucket" "s3ms" {
  name = "s3ms-tb"
}

resource "aws_s3tables_table_bucket_policy" "s3ms" {
  table_bucket_arn = aws_s3tables_table_bucket.s3ms.arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "s3ms"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "s3tables:GetTableBucket"
      Resource  = aws_s3tables_table_bucket.s3ms.arn
    }]
  })
}

resource "aws_s3tables_namespace" "s3ms" {
  namespace        = "s3ms_ns"
  table_bucket_arn = aws_s3tables_table_bucket.s3ms.arn
}

resource "aws_s3tables_table" "s3ms" {
  name             = "s3ms_table"
  namespace        = aws_s3tables_namespace.s3ms.namespace
  table_bucket_arn = aws_s3tables_table_bucket.s3ms.arn
  format           = "ICEBERG"
}

resource "aws_s3tables_table_policy" "s3ms" {
  name             = aws_s3tables_table.s3ms.name
  namespace        = aws_s3tables_namespace.s3ms.namespace
  table_bucket_arn = aws_s3tables_table_bucket.s3ms.arn
  resource_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "s3ms"
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "s3tables:GetTable"
      Resource  = aws_s3tables_table.s3ms.arn
    }]
  })
}

##############################################################################
# Service Discovery: public + private DNS namespaces (own VPC).
##############################################################################

resource "aws_vpc" "s3ms" {
  cidr_block = "10.211.0.0/16"
}

resource "aws_subnet" "s3ms" {
  vpc_id            = aws_vpc.s3ms.id
  cidr_block        = "10.211.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_service_discovery_public_dns_namespace" "s3ms" {
  name = "s3ms.example.com"
}

resource "aws_service_discovery_private_dns_namespace" "s3ms" {
  name = "s3ms.private"
  vpc  = aws_vpc.s3ms.id
}

##############################################################################
# SNS: SMS preferences (account singleton) + topic data protection policy.
##############################################################################

resource "aws_sns_topic" "s3ms" {
  name = "s3ms-topic"
}

resource "aws_sns_sms_preferences" "s3ms" {
  default_sms_type = "Transactional"
}

resource "aws_sns_topic_data_protection_policy" "s3ms" {
  arn = aws_sns_topic.s3ms.arn
  policy = jsonencode({
    Name      = "s3ms-dpp"
    Version   = "2021-06-01"
    Statement = []
  })
}

##############################################################################
# SQS: separate redrive-policy attachment resource against a DLQ.
##############################################################################

resource "aws_sqs_queue" "s3ms_dlq" {
  name = "s3ms-dlq"
}

resource "aws_sqs_queue" "s3ms_src" {
  name = "s3ms-src"
}

resource "aws_sqs_queue_redrive_policy" "s3ms" {
  queue_url = aws_sqs_queue.s3ms_src.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.s3ms_dlq.arn
    maxReceiveCount     = 5
  })
}

##############################################################################
# Transcribe: vocabulary filter.
##############################################################################

resource "aws_transcribe_vocabulary_filter" "s3ms" {
  vocabulary_filter_name = "s3ms-filter"
  language_code          = "en-US"
  words                  = ["foo", "bar"]
}

##############################################################################
# CodePipeline: custom action type.
##############################################################################

resource "aws_codepipeline_custom_action_type" "s3ms" {
  category      = "Build"
  provider_name = "s3ms-provider"
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

resource "aws_api_gateway_rest_api" "s3ms" {
  name = "s3ms-api"
}

resource "aws_api_gateway_rest_api_put" "s3ms" {
  rest_api_id      = aws_api_gateway_rest_api.s3ms.id
  fail_on_warnings = false

  parameters = {
    mode = "merge"
  }

  body = jsonencode({
    openapi = "3.0.1"
    info = {
      title   = "s3ms-api"
      version = "1.0"
    }
    paths = {
      "/s3ms" = {
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

resource "aws_vpc" "s3ms_elb" {
  cidr_block = "10.212.0.0/16"
}

resource "aws_subnet" "s3ms_elb" {
  vpc_id            = aws_vpc.s3ms_elb.id
  cidr_block        = "10.212.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_instance" "s3ms" {
  ami           = "ami-12345678"
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.s3ms_elb.id

  tags = {
    Name = "s3ms-instance"
  }
}

resource "aws_elb" "s3ms" {
  name               = "s3ms-lb"
  availability_zones = ["us-east-1a"]

  listener {
    instance_port     = 80
    instance_protocol = "HTTP"
    lb_port           = 80
    lb_protocol       = "HTTP"
  }
}

resource "aws_elb_attachment" "s3ms" {
  elb      = aws_elb.s3ms.id
  instance = aws_instance.s3ms.id
}
