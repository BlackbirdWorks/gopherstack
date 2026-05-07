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
    iot = var.gopherstack_endpoint
    sts = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack IoT endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Thing Type
# ---------------------------------------------------------------------------

resource "aws_iot_thing_type" "sensor" {
  name = "gopherstack-test-sensor-type"

  properties {
    description = "A sensor thing type for testing"
  }
}

# ---------------------------------------------------------------------------
# Thing Group
# ---------------------------------------------------------------------------

resource "aws_iot_thing_group" "fleet" {
  name = "gopherstack-test-fleet"

  properties {
    description = "Top-level fleet group"
  }
}

resource "aws_iot_thing_group" "region_east" {
  name             = "gopherstack-test-region-east"
  parent_group_name = aws_iot_thing_group.fleet.name

  properties {
    description = "Eastern region sub-group"
  }
}

# ---------------------------------------------------------------------------
# Things
# ---------------------------------------------------------------------------

resource "aws_iot_thing" "device_one" {
  name            = "gopherstack-test-device-one"
  thing_type_name = aws_iot_thing_type.sensor.name

  attributes = {
    location = "warehouse-a"
    firmware = "1.0.0"
  }
}

resource "aws_iot_thing" "device_two" {
  name = "gopherstack-test-device-two"
}

# ---------------------------------------------------------------------------
# Thing Group Membership
# ---------------------------------------------------------------------------

resource "aws_iot_thing_group_membership" "device_one_fleet" {
  thing_name       = aws_iot_thing.device_one.name
  thing_group_name = aws_iot_thing_group.fleet.name
}

# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

resource "aws_iot_policy" "device_policy" {
  name = "gopherstack-test-device-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive"]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------------------
# Certificate (from CSR placeholder)
# ---------------------------------------------------------------------------

resource "aws_iot_certificate" "device_cert" {
  csr    = file("${path.module}/device.csr")
  active = true
}

# ---------------------------------------------------------------------------
# Policy attachment to certificate
# ---------------------------------------------------------------------------

resource "aws_iot_policy_attachment" "cert_policy" {
  policy = aws_iot_policy.device_policy.name
  target = aws_iot_certificate.device_cert.arn
}

# ---------------------------------------------------------------------------
# Topic Rule
# ---------------------------------------------------------------------------

resource "aws_iot_topic_rule" "temperature_rule" {
  name        = "gopherstack_test_temp_rule"
  description = "Forward temperature readings"
  enabled     = true
  sql         = "SELECT temperature FROM 'sensors/#'"
  sql_version = "2015-10-08"

  # No-op error action required by AWS provider
  error_action {
    sqs {
      queue_url  = "https://sqs.us-east-1.amazonaws.com/000000000000/error-queue"
      role_arn   = "arn:aws:iam::000000000000:role/iot-role"
      use_base64 = false
    }
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "thing_arn" {
  description = "ARN of device_one"
  value       = aws_iot_thing.device_one.arn
}

output "thing_group_arn" {
  description = "ARN of fleet group"
  value       = aws_iot_thing_group.fleet.arn
}

output "policy_arn" {
  description = "ARN of device policy"
  value       = aws_iot_policy.device_policy.arn
}

output "certificate_arn" {
  description = "ARN of device certificate"
  value       = aws_iot_certificate.device_cert.arn
}

output "topic_rule_arn" {
  description = "ARN of temperature rule"
  value       = aws_iot_topic_rule.temperature_rule.arn
}
