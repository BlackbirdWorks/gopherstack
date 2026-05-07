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
    s3  = var.gopherstack_endpoint
    sqs = var.gopherstack_endpoint
    sns = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# SQS queue for bucket notifications
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "notify" {
  name = "gopherstack-s3-notify"
}

# ---------------------------------------------------------------------------
# Primary bucket with versioning enabled
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "primary" {
  bucket = "gopherstack-test-primary"

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
# Bucket with Object Lock (requires versioning)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "locked" {
  bucket = "gopherstack-test-locked"

  tags = {
    Environment = "test"
    ManagedBy   = "terraform"
    Purpose     = "object-lock"
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
}

# ---------------------------------------------------------------------------
# Bucket ownership controls
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "owned" {
  bucket = "gopherstack-test-owned"
}

resource "aws_s3_bucket_ownership_controls" "owned" {
  bucket = aws_s3_bucket.owned.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# ---------------------------------------------------------------------------
# Bucket notifications — SQS target
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_notification" "primary" {
  bucket = aws_s3_bucket.primary.id

  queue {
    id        = "all-object-events"
    queue_arn = aws_sqs_queue.notify.arn
    events    = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }
}

# ---------------------------------------------------------------------------
# Bucket intelligent-tiering configuration
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_intelligent_tiering_configuration" "primary" {
  bucket = aws_s3_bucket.primary.id
  name   = "primary-tiering"
  status = "Enabled"

  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
}

# ---------------------------------------------------------------------------
# Bucket replication configuration (config-only; no actual cross-region copy)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "replica" {
  bucket = "gopherstack-test-replica"
}

resource "aws_s3_bucket_versioning" "replica" {
  bucket = aws_s3_bucket.replica.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_replication_configuration" "primary" {
  bucket = aws_s3_bucket.primary.id
  role   = "arn:aws:iam::000000000000:role/replication-role"

  rule {
    id     = "replicate-all"
    status = "Enabled"

    destination {
      bucket        = aws_s3_bucket.replica.arn
      storage_class = "STANDARD"
    }
  }

  depends_on = [aws_s3_bucket_versioning.primary]
}

# ---------------------------------------------------------------------------
# An S3 object in the primary bucket
# ---------------------------------------------------------------------------

resource "aws_s3_object" "hello" {
  bucket  = aws_s3_bucket.primary.id
  key     = "hello.txt"
  content = "Hello from gopherstack!"

  depends_on = [aws_s3_bucket_versioning.primary]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "primary_bucket" {
  value = aws_s3_bucket.primary.id
}

output "locked_bucket" {
  value = aws_s3_bucket.locked.id
}

output "notify_queue_url" {
  value = aws_sqs_queue.notify.url
}
