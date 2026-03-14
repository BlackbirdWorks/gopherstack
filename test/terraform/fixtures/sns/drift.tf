resource "aws_sns_topic" "this" {
  name         = "{{.TopicName}}"
  display_name = "initial-name"
}
