resource "aws_iot_thing" "this" {
  name = "{{.ThingName}}"
}
