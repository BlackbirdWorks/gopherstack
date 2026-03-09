resource "aws_sesv2_email_identity" "this" {
  email_identity = "{{.Email}}"
}

resource "aws_sesv2_configuration_set" "this" {
  configuration_set_name = "{{.ConfigSetName}}"
}
