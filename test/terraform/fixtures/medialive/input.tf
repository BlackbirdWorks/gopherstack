resource "aws_medialive_input" "this" {
  name = "{{.InputName}}"
  type = "RTMP_PUSH"

  destinations {
    stream_name = "live/stream"
  }

  tags = {
    Environment = "test"
  }
}
