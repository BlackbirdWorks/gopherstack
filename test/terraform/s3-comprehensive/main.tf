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
    s3 = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# Primary bucket with versioning — used for inventory, metrics, analytics
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "primary" {
  bucket = "gopherstack-comprehensive-primary"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "primary" {
  bucket = aws_s3_bucket.primary.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------
# Inventory destination bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "inventory_dest" {
  bucket = "gopherstack-comprehensive-inventory-dest"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Purpose     = "inventory-destination"
  }
}

# ---------------------------------------------------------------------------
# Bucket inventory configuration
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_inventory" "daily" {
  bucket = aws_s3_bucket.primary.id
  name   = "daily-inventory"

  included_object_versions = "All"

  schedule {
    frequency = "Daily"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.inventory_dest.arn
    }
  }

  optional_fields = ["Size", "LastModifiedDate", "StorageClass", "ETag"]
}

resource "aws_s3_bucket_inventory" "weekly" {
  bucket = aws_s3_bucket.primary.id
  name   = "weekly-inventory"

  included_object_versions = "Current"

  schedule {
    frequency = "Weekly"
  }

  destination {
    bucket {
      format     = "ORC"
      bucket_arn = aws_s3_bucket.inventory_dest.arn
      prefix     = "weekly/"
    }
  }

  optional_fields = ["Size", "ETag"]
}

# ---------------------------------------------------------------------------
# Bucket metrics configurations
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_metric" "all_objects" {
  bucket = aws_s3_bucket.primary.id
  name   = "all-objects-metrics"
}

resource "aws_s3_bucket_metric" "filtered" {
  bucket = aws_s3_bucket.primary.id
  name   = "filtered-metrics"

  filter {
    prefix = "logs/"

    tags = {
      priority = "high"
    }
  }
}

# ---------------------------------------------------------------------------
# Bucket analytics configurations
# ---------------------------------------------------------------------------

resource "aws_s3_analytics_configuration" "primary" {
  bucket = aws_s3_bucket.primary.id
  name   = "primary-analytics"

  storage_class_analysis {
    data_export {
      destination {
        s3_bucket_destination {
          bucket_arn = aws_s3_bucket.inventory_dest.arn
        }
      }
    }
  }
}

resource "aws_s3_analytics_configuration" "logs" {
  bucket = aws_s3_bucket.primary.id
  name   = "logs-analytics"

  filter {
    prefix = "logs/"
  }
}

# ---------------------------------------------------------------------------
# Object Lock bucket for legal hold testing
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "locked" {
  bucket              = "gopherstack-comprehensive-locked"
  object_lock_enabled = true

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Purpose     = "legal-hold"
  }
}

resource "aws_s3_bucket_versioning" "locked" {
  bucket = aws_s3_bucket.locked.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_object_lock_configuration" "locked" {
  bucket = aws_s3_bucket.locked.id

  object_lock_enabled = "Enabled"

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.locked]
}

# ---------------------------------------------------------------------------
# Objects with tags
# ---------------------------------------------------------------------------

resource "aws_s3_object" "tagged" {
  bucket  = aws_s3_bucket.primary.id
  key     = "data/tagged-object.txt"
  content = "This object has tags."

  tags = {
    project  = "gopherstack"
    category = "test"
    tier     = "hot"
  }

  depends_on = [aws_s3_bucket_versioning.primary]
}

resource "aws_s3_object" "logs_object" {
  bucket  = aws_s3_bucket.primary.id
  key     = "logs/app.log"
  content = "INFO: application started\nINFO: processing complete\n"

  tags = {
    log-level = "info"
    retention = "30d"
  }

  depends_on = [aws_s3_bucket_versioning.primary]
}

# ---------------------------------------------------------------------------
# Object in locked bucket (legal hold can be applied via API)
# ---------------------------------------------------------------------------

resource "aws_s3_object" "locked_object" {
  bucket  = aws_s3_bucket.locked.id
  key     = "protected/sensitive.txt"
  content = "Sensitive data under legal hold."

  depends_on = [aws_s3_bucket_object_lock_configuration.locked]
}

# ---------------------------------------------------------------------------
# Bucket tags
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_tagging" "primary" {
  bucket = aws_s3_bucket.primary.id

  tagging {
    tag_set {
      key   = "CostCenter"
      value = "engineering"
    }

    tag_set {
      key   = "Project"
      value = "gopherstack"
    }
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "primary_bucket" {
  value = aws_s3_bucket.primary.id
}

output "inventory_dest_bucket" {
  value = aws_s3_bucket.inventory_dest.id
}

output "locked_bucket" {
  value = aws_s3_bucket.locked.id
}

output "tagged_object_key" {
  value = aws_s3_object.tagged.key
}

output "locked_object_key" {
  value = aws_s3_object.locked_object.key
}

output "daily_inventory_id" {
  value = aws_s3_bucket_inventory.daily.name
}

output "weekly_inventory_id" {
  value = aws_s3_bucket_inventory.weekly.name
}

output "metrics_all_objects_id" {
  value = aws_s3_bucket_metric.all_objects.name
}

output "analytics_primary_id" {
  value = aws_s3_analytics_configuration.primary.name
}
