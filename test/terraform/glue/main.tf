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
    glue = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# IAM role used by Glue resources (mocked, no real IAM validation needed)
# ---------------------------------------------------------------------------
locals {
  glue_role_arn = "arn:aws:iam::000000000000:role/GlueServiceRole"
}

# ---------------------------------------------------------------------------
# Glue Catalog Database (prerequisite for crawler)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "test_db" {
  name = "gopherstack_test_db"
}

# ---------------------------------------------------------------------------
# Glue Crawler
# ---------------------------------------------------------------------------
resource "aws_glue_crawler" "test_crawler" {
  name          = "gopherstack-test-crawler"
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.test_db.name

  s3_target {
    path = "s3://example-bucket/data/"
  }
}

# ---------------------------------------------------------------------------
# Glue Job
# ---------------------------------------------------------------------------
resource "aws_glue_job" "test_job" {
  name     = "gopherstack-test-job"
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://example-bucket/scripts/etl.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

# ---------------------------------------------------------------------------
# Glue Trigger (ON_DEMAND)
# ---------------------------------------------------------------------------
resource "aws_glue_trigger" "test_trigger" {
  name = "gopherstack-test-trigger"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.test_job.name
  }
}

# ---------------------------------------------------------------------------
# Glue Workflow
# ---------------------------------------------------------------------------
resource "aws_glue_workflow" "test_workflow" {
  name        = "gopherstack-test-workflow"
  description = "Test workflow for gopherstack"
}

# ---------------------------------------------------------------------------
# Glue Trigger attached to workflow (SCHEDULED)
# ---------------------------------------------------------------------------
resource "aws_glue_trigger" "scheduled_trigger" {
  name          = "gopherstack-scheduled-trigger"
  type          = "SCHEDULED"
  schedule      = "cron(0 12 * * ? *)"
  workflow_name = aws_glue_workflow.test_workflow.name

  actions {
    job_name = aws_glue_job.test_job.name
  }
}

# ---------------------------------------------------------------------------
# Glue Classifier (Grok)
# ---------------------------------------------------------------------------
resource "aws_glue_classifier" "test_classifier" {
  name = "gopherstack-test-classifier"

  grok_classifier {
    classification = "example"
    grok_pattern   = "%%{SYSLOGTIMESTAMP:timestamp}"
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "crawler_name" {
  value = aws_glue_crawler.test_crawler.name
}

output "job_name" {
  value = aws_glue_job.test_job.name
}

output "trigger_name" {
  value = aws_glue_trigger.test_trigger.name
}

output "workflow_name" {
  value = aws_glue_workflow.test_workflow.name
}

output "classifier_name" {
  value = aws_glue_classifier.test_classifier.name
}
