resource "aws_s3_bucket" "this" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_sqs_queue" "this" {
  name = "{{.QueueName}}"
}
