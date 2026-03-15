resource "aws_sns_topic" "this" {
  name = "{{.TopicName}}"
}
