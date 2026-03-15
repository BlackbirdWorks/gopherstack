resource "aws_sqs_queue" "this" {
  name                       = "{{.QueueName}}"
  visibility_timeout_seconds = 30
}
