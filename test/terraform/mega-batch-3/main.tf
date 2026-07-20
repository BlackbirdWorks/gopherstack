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
    glue        = var.gopherstack_endpoint
    athena      = var.gopherstack_endpoint
    opensearch  = var.gopherstack_endpoint
    redshiftdata = var.gopherstack_endpoint
    iam         = var.gopherstack_endpoint
    s3          = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Glue Database (required by crawler)
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "analytics" {
  name = "analytics_db"
}

# ---------------------------------------------------------------------------
# Glue Crawler
# ---------------------------------------------------------------------------

resource "aws_glue_crawler" "s3_crawler" {
  name          = "s3-analytics-crawler"
  role          = "arn:aws:iam::000000000000:role/GlueCrawlerRole"
  database_name = aws_glue_catalog_database.analytics.name

  s3_target {
    path = "s3://my-data-bucket/raw/"
  }

  tags = {
    Environment = "test"
    Project     = "mega-batch-3"
  }
}

# ---------------------------------------------------------------------------
# Glue Job
# ---------------------------------------------------------------------------

resource "aws_glue_job" "etl_job" {
  name     = "etl-transform-job"
  role_arn = "arn:aws:iam::000000000000:role/GlueJobRole"

  command {
    name            = "glueetl"
    script_location = "s3://my-scripts-bucket/etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--enable-metrics"      = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  number_of_workers = 2
  worker_type       = "G.1X"
  glue_version      = "4.0"
  max_retries       = 1
  timeout           = 60

  tags = {
    Environment = "test"
    Project     = "mega-batch-3"
  }
}

# ---------------------------------------------------------------------------
# Glue Schema Registry
# ---------------------------------------------------------------------------

resource "aws_glue_registry" "analytics_registry" {
  registry_name = "analytics-schema-registry"
  description   = "Schema registry for analytics events"

  tags = {
    Environment = "test"
  }
}

resource "aws_glue_schema" "event_schema" {
  schema_name       = "analytics-event-schema"
  registry_arn      = aws_glue_registry.analytics_registry.arn
  data_format       = "AVRO"
  compatibility     = "BACKWARD"
  schema_definition = jsonencode({
    type = "record"
    name = "AnalyticsEvent"
    fields = [
      { name = "event_id", type = "string" },
      { name = "timestamp", type = "long" },
      { name = "event_type", type = "string" },
      { name = "user_id", type = "string" },
    ]
  })

  tags = {
    Environment = "test"
  }
}

# ---------------------------------------------------------------------------
# Athena Workgroup
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "analytics" {
  name        = "analytics-workgroup"
  description = "Workgroup for analytics queries"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://my-athena-results/output/"
    }
  }

  tags = {
    Environment = "test"
    Project     = "mega-batch-3"
  }
}

# ---------------------------------------------------------------------------
# Athena Named Query
# ---------------------------------------------------------------------------

resource "aws_athena_named_query" "list_events" {
  name        = "list-analytics-events"
  description = "Lists all analytics events from the last 7 days"
  workgroup   = aws_athena_workgroup.analytics.name
  database    = aws_glue_catalog_database.analytics.name
  query       = "SELECT * FROM analytics_events WHERE dt >= date_add('day', -7, current_date) LIMIT 100;"
}

# ---------------------------------------------------------------------------
# OpenSearch Domain
# ---------------------------------------------------------------------------

resource "aws_opensearch_domain" "analytics" {
  domain_name    = "analytics-search"
  engine_version = "OpenSearch_2.11"

  cluster_config {
    instance_type  = "r6g.large.search"
    instance_count = 1
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  ebs_options {
    ebs_enabled = true
    volume_type = "gp3"
    volume_size = 20
  }

  tags = {
    Environment = "test"
    Project     = "mega-batch-3"
  }
}
