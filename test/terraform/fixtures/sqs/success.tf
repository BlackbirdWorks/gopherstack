resource "aws_sqs_queue" "this" {
  name                       = "{{.QueueName}}"
  visibility_timeout_seconds = 30
}

resource "aws_sqs_queue" "fifo" {
  name                        = "{{.FIFOQueueName}}.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
}
