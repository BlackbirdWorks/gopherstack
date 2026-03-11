resource "aws_cloudtrail" "this" {
  name           = "{{.TrailName}}"
  s3_bucket_name = aws_s3_bucket.trail_bucket.id

  enable_log_file_validation    = true
  is_multi_region_trail         = false
  include_global_service_events = true

  depends_on = [aws_s3_bucket_policy.this]

  tags = {
    Environment = "test"
  }
}

resource "aws_s3_bucket" "trail_bucket" {
  bucket = "{{.BucketName}}"
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.trail_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AWSCloudTrailAclCheck"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:GetBucketAcl"
        Resource = aws_s3_bucket.trail_bucket.arn
      },
      {
        Sid    = "AWSCloudTrailWrite"
        Effect = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.trail_bucket.arn}/AWSLogs/*"
      },
    ]
  })
}
