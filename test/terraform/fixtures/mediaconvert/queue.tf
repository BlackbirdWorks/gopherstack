resource "aws_media_convert_queue" "this" {
  name   = "tf-mc-queue-{{.QueueName}}"
  status = "ACTIVE"
}
