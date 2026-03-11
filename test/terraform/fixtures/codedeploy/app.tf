resource "aws_codedeploy_app" "this" {
  name             = "{{.AppName}}"
  compute_platform = "Server"
}
