resource "aws_sqs_queue" "fifo" {
  name                          = "{{.QueueName}}.fifo"
  fifo_queue                    = true
  content_based_deduplication   = true
  deduplication_scope           = "messageGroup"
  fifo_throughput_limit         = "perMessageGroupId"
  visibility_timeout_seconds    = 30
}
