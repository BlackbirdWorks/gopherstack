resource "aws_kinesis_analytics_application" "this" {
  name = "{{.AppName}}"

  tags = {
    Environment = "test"
  }
}
