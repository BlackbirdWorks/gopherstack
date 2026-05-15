resource "aws_s3_bucket" "this" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

# Default server-side encryption with AES256 (SSE-S3). With real envelope
# encryption wired in (see services/s3/sse_crypto.go), objects placed in this
# bucket are actually ciphertext at rest — beyond LocalStack's metadata echo.
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
