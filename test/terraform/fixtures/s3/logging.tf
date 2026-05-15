resource "aws_s3_bucket" "source" {
  bucket        = "{{.SourceBucket}}"
  force_destroy = true
}

resource "aws_s3_bucket" "logs" {
  bucket        = "{{.LogBucket}}"
  force_destroy = true
}

resource "aws_s3_bucket_logging" "source" {
  bucket        = aws_s3_bucket.source.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "access-logs/"
}
