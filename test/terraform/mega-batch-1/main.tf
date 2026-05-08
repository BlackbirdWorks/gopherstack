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
    cognitoidentityprovider = var.gopherstack_endpoint
    config                  = var.gopherstack_endpoint
    cloudtrail              = var.gopherstack_endpoint
    ecr                     = var.gopherstack_endpoint
    sns                     = var.gopherstack_endpoint
    sqs                     = var.gopherstack_endpoint
    lambda                  = var.gopherstack_endpoint
    route53                 = var.gopherstack_endpoint
    iam                     = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# 1. Cognito — User pool with groups and users
# ---------------------------------------------------------------------------

resource "aws_cognito_user_pool" "main" {
  name = "mega-batch-pool"

  password_policy {
    minimum_length    = 8
    require_lowercase = true
    require_numbers   = true
    require_symbols   = false
    require_uppercase = true
  }
}

resource "aws_cognito_user_pool_client" "main" {
  name         = "mega-batch-client"
  user_pool_id = aws_cognito_user_pool.main.id

  explicit_auth_flows = [
    "ALLOW_USER_PASSWORD_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
  ]
}

resource "aws_cognito_user_group" "admins" {
  name         = "admins"
  user_pool_id = aws_cognito_user_pool.main.id
  description  = "Administrator group"
  precedence   = 1
}

resource "aws_cognito_user_group" "users" {
  name         = "users"
  user_pool_id = aws_cognito_user_pool.main.id
  description  = "Regular users"
  precedence   = 10
}

# ---------------------------------------------------------------------------
# 2. ECR — Repository with pull-through cache rule
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "app" {
  name                 = "mega-batch/app"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_pull_through_cache_rule" "dockerhub" {
  ecr_repository_prefix = "dockerhub"
  upstream_registry_url = "registry-1.docker.io"
}

# ---------------------------------------------------------------------------
# 3. CloudTrail — Trail with insight selectors and event data store
# ---------------------------------------------------------------------------

resource "aws_cloudtrail" "main" {
  name                          = "mega-batch-trail"
  s3_bucket_name                = "mega-batch-cloudtrail-logs"
  include_global_service_events = true
  is_multi_region_trail         = false
  enable_log_file_validation    = true
}

resource "aws_cloudtrail_event_data_store" "main" {
  name                 = "mega-batch-eds"
  retention_period     = 90
  multi_region_enabled = false

  advanced_event_selector {
    name = "All S3 events"

    field_selector {
      field  = "eventCategory"
      equals = ["Data"]
    }

    field_selector {
      field  = "resources.type"
      equals = ["AWS::S3::Object"]
    }
  }
}

# ---------------------------------------------------------------------------
# 4. AWS Config — Config rules with source and parameters
# ---------------------------------------------------------------------------

resource "aws_config_configuration_recorder" "main" {
  name     = "mega-batch-recorder"
  role_arn = aws_iam_role.config.arn
}

resource "aws_config_delivery_channel" "main" {
  name           = "mega-batch-channel"
  s3_bucket_name = "mega-batch-config-logs"

  depends_on = [aws_config_configuration_recorder.main]
}

resource "aws_config_config_rule" "s3_public_access" {
  name        = "s3-bucket-public-access-prohibited"
  description = "Checks that S3 buckets do not allow public access"

  source {
    owner             = "AWS"
    source_identifier = "S3_BUCKET_PUBLIC_READ_PROHIBITED"
  }

  depends_on = [aws_config_configuration_recorder.main]
}

resource "aws_config_config_rule" "custom_rule" {
  name             = "custom-lambda-rule"
  description      = "Custom rule via Lambda"
  input_parameters = jsonencode({ "maxRetentionDays" = 30 })

  source {
    owner = "CUSTOM_LAMBDA"

    source_identifier = "arn:aws:lambda:us-east-1:123456789012:function:config-evaluator"

    source_detail {
      event_source = "aws.config"
      message_type = "ConfigurationItemChangeNotification"
    }
  }

  maximum_execution_frequency = "TwentyFour_Hours"

  depends_on = [aws_config_configuration_recorder.main]
}

# ---------------------------------------------------------------------------
# 5. SNS — Standard and FIFO topics
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "standard" {
  name = "mega-batch-standard"
}

resource "aws_sns_topic" "fifo" {
  name                        = "mega-batch-fifo.fifo"
  fifo_topic                  = true
  content_based_deduplication = true
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.standard.arn
  protocol  = "email"
  endpoint  = "ops@example.com"
}

# ---------------------------------------------------------------------------
# 6. SQS — Standard and FIFO queues
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "standard" {
  name                      = "mega-batch-standard"
  message_retention_seconds = 86400
  visibility_timeout_seconds = 30
}

resource "aws_sqs_queue" "fifo" {
  name                        = "mega-batch-fifo.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
}

resource "aws_sqs_queue" "dlq" {
  name = "mega-batch-dlq"
}

resource "aws_sqs_queue_redrive_policy" "standard" {
  queue_url = aws_sqs_queue.standard.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

# ---------------------------------------------------------------------------
# 7. Lambda — Functions with reserved concurrency and event invoke config
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "mega-batch-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_lambda_function" "processor" {
  function_name = "mega-batch-processor"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.12"
  handler       = "index.handler"
  filename      = "/dev/null"

  environment {
    variables = {
      QUEUE_URL = aws_sqs_queue.standard.url
    }
  }
}

resource "aws_lambda_function_event_invoke_config" "processor" {
  function_name          = aws_lambda_function.processor.function_name
  maximum_retry_attempts = 1
  maximum_event_age_in_seconds = 300
}

resource "aws_lambda_function" "worker" {
  function_name = "mega-batch-worker"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.12"
  handler       = "index.handler"
  filename      = "/dev/null"
}

resource "aws_lambda_function_event_invoke_config" "worker" {
  function_name          = aws_lambda_function.worker.function_name
  maximum_retry_attempts = 2
  maximum_event_age_in_seconds = 600
}

# ---------------------------------------------------------------------------
# 8. Route53 — Hosted zone with latency and geolocation routing
# ---------------------------------------------------------------------------

resource "aws_route53_zone" "main" {
  name = "mega-batch.example.com"
}

resource "aws_route53_record" "latency_us" {
  zone_id        = aws_route53_zone.main.zone_id
  name           = "api.mega-batch.example.com"
  type           = "A"
  set_identifier = "us-east-1"
  ttl            = 60

  latency_routing_policy {
    region = "us-east-1"
  }

  records = ["10.0.1.1"]
}

resource "aws_route53_record" "latency_eu" {
  zone_id        = aws_route53_zone.main.zone_id
  name           = "api.mega-batch.example.com"
  type           = "A"
  set_identifier = "eu-west-1"
  ttl            = 60

  latency_routing_policy {
    region = "eu-west-1"
  }

  records = ["10.0.2.1"]
}

resource "aws_route53_record" "geo_us" {
  zone_id        = aws_route53_zone.main.zone_id
  name           = "cdn.mega-batch.example.com"
  type           = "CNAME"
  set_identifier = "us"
  ttl            = 300

  geolocation_routing_policy {
    country = "US"
  }

  records = ["us-cdn.example.com"]
}

resource "aws_route53_record" "geo_default" {
  zone_id        = aws_route53_zone.main.zone_id
  name           = "cdn.mega-batch.example.com"
  type           = "CNAME"
  set_identifier = "default"
  ttl            = 300

  geolocation_routing_policy {
    country = "*"
  }

  records = ["global-cdn.example.com"]
}

# ---------------------------------------------------------------------------
# Supporting IAM resources
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "config_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["config.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "config" {
  name               = "mega-batch-config-role"
  assume_role_policy = data.aws_iam_policy_document.config_assume.json
}
