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
    events = var.gopherstack_endpoint
    sqs    = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack EventBridge endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Custom event bus
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_bus" "orders" {
  name = "gopherstack-orders"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Rule on the custom bus
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "order_placed" {
  name           = "gopherstack-order-placed"
  event_bus_name = aws_cloudwatch_event_bus.orders.name
  description    = "Fires when an order is placed"

  event_pattern = jsonencode({
    source      = ["com.gopherstack.orders"]
    detail-type = ["OrderPlaced"]
  })

  state = "ENABLED"

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# SQS queue as a target
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "order_events" {
  name = "gopherstack-order-events"

  tags = {
    Environment = "test"
  }
}

resource "aws_cloudwatch_event_target" "order_placed_sqs" {
  rule           = aws_cloudwatch_event_rule.order_placed.name
  event_bus_name = aws_cloudwatch_event_bus.orders.name
  target_id      = "order-placed-sqs"
  arn            = aws_sqs_queue.order_events.arn
}

# ---------------------------------------------------------------------------
# Archive for the custom bus
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_archive" "orders_archive" {
  name             = "gopherstack-orders-archive"
  event_source_arn = aws_cloudwatch_event_bus.orders.arn
  description      = "Archive of all order events"
  retention_days   = 7
}

# ---------------------------------------------------------------------------
# Connection (HTTP basic auth)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_connection" "webhook" {
  name               = "gopherstack-webhook"
  description        = "Webhook connection for testing"
  authorization_type = "BASIC"

  auth_parameters {
    basic {
      username = "testuser"
      password = "testpassword"
    }
  }
}

# ---------------------------------------------------------------------------
# API destination
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_api_destination" "webhook_dest" {
  name                             = "gopherstack-webhook-dest"
  description                      = "Webhook API destination"
  connection_arn                   = aws_cloudwatch_event_connection.webhook.arn
  invocation_endpoint              = "https://example.com/webhook"
  http_method                      = "POST"
  invocation_rate_limit_per_second = 10
}

# ---------------------------------------------------------------------------
# Rule on the default bus
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "default_rule" {
  name        = "gopherstack-default-rule"
  description = "Rule on the default event bus"

  event_pattern = jsonencode({
    source = ["com.gopherstack.default"]
  })

  state = "ENABLED"
}

resource "aws_cloudwatch_event_target" "default_sqs" {
  rule      = aws_cloudwatch_event_rule.default_rule.name
  target_id = "default-sqs"
  arn       = aws_sqs_queue.order_events.arn
}
