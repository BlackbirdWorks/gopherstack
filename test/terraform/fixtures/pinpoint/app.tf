resource "aws_pinpoint_app" "this" {
  name = "{{.AppName}}"

  tags = {
    Environment = "test"
  }
}
