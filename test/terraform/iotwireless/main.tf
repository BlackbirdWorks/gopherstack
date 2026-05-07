terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"

  access_key = "test"
  secret_key = "test"

  # Point at local gopherstack instance
  endpoints {
    iotwireless = "http://localhost:4566"
  }

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

# --- Destination ---

resource "aws_iotwireless_destination" "example" {
  name            = "tf-test-destination"
  expression      = "LocationEventsTopic"
  expression_type = "MqttTopic"
  role_arn        = "arn:aws:iam::123456789012:role/IoTWirelessRole"
  description     = "Terraform test destination"

  tags = {
    Environment = "test"
  }
}

output "destination_arn" {
  value = aws_iotwireless_destination.example.arn
}

# --- Wireless Gateway ---

resource "aws_iotwireless_gateway" "example" {
  name        = "tf-test-gateway"
  description = "Terraform test wireless gateway"

  lo_ra_wan {
    gateway_eui  = "a1b2c3d4e5f6a1b2"
    rf_region    = "US915"
  }

  tags = {
    Environment = "test"
  }
}

output "gateway_id" {
  value = aws_iotwireless_gateway.example.id
}

output "gateway_arn" {
  value = aws_iotwireless_gateway.example.arn
}
