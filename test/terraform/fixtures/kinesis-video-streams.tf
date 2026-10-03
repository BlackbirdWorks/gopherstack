resource "aws_kinesis_video_stream" "this" {
  name                    = "{{.StreamName}}"
  data_retention_in_hours = 48
  device_name             = "tf-kvs-device"
  media_type              = "video/h264"

  tags = {
    Environment = "test"
    Owner       = "terraform"
  }
}
