data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_iam_policy_document" "example" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["*"]
  }
}

resource "aws_s3_bucket" "this" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

data "aws_s3_bucket" "this" {
  bucket     = aws_s3_bucket.this.bucket
  depends_on = [aws_s3_bucket.this]
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = data.aws_region.current.name
}

output "policy_json" {
  value = data.aws_iam_policy_document.example.json
}
