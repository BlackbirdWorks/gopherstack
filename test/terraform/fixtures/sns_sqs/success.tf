resource "aws_sns_topic" "this" {
  name = "{{.TopicName}}"
}

resource "aws_sqs_queue" "this" {
  name = "{{.QueueName}}"
}

resource "aws_sns_topic_subscription" "this" {
  topic_arn = aws_sns_topic.this.arn
  protocol  = "sqs"
  endpoint  = "arn:aws:sqs:us-east-1:000000000000:${aws_sqs_queue.this.name}"
}
