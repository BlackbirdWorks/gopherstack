resource "aws_iam_role" "comprehend" {
  name = "{{.RoleName}}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "comprehend.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_s3_bucket" "data" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_comprehend_document_classifier" "this" {
  name               = "{{.ClassifierName}}"
  data_access_role_arn = aws_iam_role.comprehend.arn
  language_code      = "en"

  input_data_config {
    s3_uri = "s3://${aws_s3_bucket.data.bucket}/train"
  }

  depends_on = [aws_iam_role.comprehend, aws_s3_bucket.data]
}
