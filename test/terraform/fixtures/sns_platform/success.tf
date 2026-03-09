resource "aws_sns_platform_application" "this" {
  name                = "{{.AppName}}"
  platform            = "GCM"
  platform_credential = "fake-gcm-server-key"
}
