resource "aws_iotwireless_service_profile" "this" {
  name = "{{.ProfileName}}"

  tags = {
    Environment = "test"
  }
}
