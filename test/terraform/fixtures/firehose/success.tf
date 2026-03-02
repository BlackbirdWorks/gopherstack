resource "aws_iam_role" "firehose" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "firehose" {
  bucket = "{{.BucketName}}"
}

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = "{{.StreamName}}"
  destination = "extended_s3"

  timeouts {
    create = "2m"
    delete = "2m"
    update = "2m"
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.firehose.arn
  }
}
