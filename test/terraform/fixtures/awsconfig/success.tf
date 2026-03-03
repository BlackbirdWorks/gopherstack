resource "aws_iam_role" "config" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "config.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "config" {
  bucket = "{{.BucketName}}"
}

resource "aws_config_configuration_recorder" "this" {
  name     = "{{.RecorderName}}"
  role_arn = aws_iam_role.config.arn
}

resource "aws_config_delivery_channel" "this" {
  name           = "{{.ChannelName}}"
  s3_bucket_name = aws_s3_bucket.config.bucket

  depends_on = [aws_config_configuration_recorder.this]
}
