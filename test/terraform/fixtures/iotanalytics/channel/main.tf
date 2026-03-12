resource "aws_iotanalytics_channel" "this" {
  name = "{{.ChannelName}}"

  tags = {
    Environment = "test"
  }
}
