resource "aws_media_package_channel" "this" {
  channel_id  = "{{.ChannelID}}"
  description = "Terraform test channel"

  tags = {
    Environment = "test"
  }
}
