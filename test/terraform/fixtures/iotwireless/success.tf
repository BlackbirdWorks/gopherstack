resource "aws_iotwireless_service_profile" "this" {
  name = "{{.ProfileName}}"
}

resource "aws_iotwireless_destination" "this" {
  expression      = "{{.RuleName}}"
  expression_type = "RuleName"
  name            = "{{.DestName}}"
  role_arn        = "arn:aws:iam::000000000000:role/IoTWirelessDestRole"
}
