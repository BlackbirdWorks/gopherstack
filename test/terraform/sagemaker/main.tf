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
    sagemaker = var.gopherstack_endpoint
    sts       = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack SageMaker endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SageMaker Model
# ---------------------------------------------------------------------------

resource "aws_sagemaker_model" "basic" {
  name               = "gopherstack-test-model"
  execution_role_arn = "arn:aws:iam::000000000000:role/sagemaker-role"

  primary_container {
    image = "000000000000.dkr.ecr.us-east-1.amazonaws.com/myimage:latest"
  }
}

# ---------------------------------------------------------------------------
# SageMaker Endpoint Configuration
# ---------------------------------------------------------------------------

resource "aws_sagemaker_endpoint_configuration" "basic" {
  name = "gopherstack-test-endpoint-config"

  production_variants {
    variant_name           = "AllTraffic"
    model_name             = aws_sagemaker_model.basic.name
    initial_instance_count = 1
    instance_type          = "ml.t2.medium"
  }
}

# ---------------------------------------------------------------------------
# SageMaker Endpoint
# ---------------------------------------------------------------------------

resource "aws_sagemaker_endpoint" "basic" {
  name                 = "gopherstack-test-endpoint"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.basic.name
}

# ---------------------------------------------------------------------------
# SageMaker Notebook Instance
# ---------------------------------------------------------------------------

resource "aws_sagemaker_notebook_instance" "basic" {
  name          = "gopherstack-test-notebook"
  role_arn      = "arn:aws:iam::000000000000:role/sagemaker-role"
  instance_type = "ml.t2.medium"

  lifecycle {
    ignore_changes = [default_code_repository]
  }
}

# ---------------------------------------------------------------------------
# SageMaker Feature Group
# ---------------------------------------------------------------------------

resource "aws_sagemaker_feature_group" "basic" {
  feature_group_name             = "gopherstack-test-feature-group"
  record_identifier_feature_name = "record-id"
  event_time_feature_name        = "event-time"
  role_arn                       = "arn:aws:iam::000000000000:role/sagemaker-role"

  feature_definition {
    feature_name = "record-id"
    feature_type = "Integral"
  }

  feature_definition {
    feature_name = "event-time"
    feature_type = "Fractional"
  }

  feature_definition {
    feature_name = "value"
    feature_type = "String"
  }

  online_store_config {
    enable_online_store = true
  }
}

# ---------------------------------------------------------------------------
# SageMaker Pipeline
# ---------------------------------------------------------------------------

resource "aws_sagemaker_pipeline" "basic" {
  pipeline_name         = "gopherstack-test-pipeline"
  pipeline_display_name = "Gopherstack Test Pipeline"
  role_arn              = "arn:aws:iam::000000000000:role/sagemaker-role"

  pipeline_definition = jsonencode({
    Version = "2020-12-01"
    Steps   = []
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "model_arn" {
  value = aws_sagemaker_model.basic.arn
}

output "endpoint_arn" {
  value = aws_sagemaker_endpoint.basic.arn
}

output "notebook_arn" {
  value = aws_sagemaker_notebook_instance.basic.arn
}

output "feature_group_arn" {
  value = aws_sagemaker_feature_group.basic.arn
}

output "pipeline_arn" {
  value = aws_sagemaker_pipeline.basic.arn
}
