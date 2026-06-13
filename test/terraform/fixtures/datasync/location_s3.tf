resource "aws_iam_role" "datasync" {
  name = "{{.RoleName}}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "datasync.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "data" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_datasync_location_s3" "this" {
  s3_bucket_arn = aws_s3_bucket.data.arn
  subdirectory  = "/data"

  s3_config {
    bucket_access_role_arn = aws_iam_role.datasync.arn
  }

  tags = {
    Environment = "test"
  }
}
