resource "aws_s3_bucket" "this" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}
