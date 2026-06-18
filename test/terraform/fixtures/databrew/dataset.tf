resource "aws_s3_bucket" "data" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}
