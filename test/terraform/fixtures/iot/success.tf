resource "aws_iot_thing" "device" {
  name = "{{.ThingName}}"
}
