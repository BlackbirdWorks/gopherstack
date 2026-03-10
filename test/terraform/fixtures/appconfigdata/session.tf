resource "aws_ssm_parameter" "appconfigdata_app" {
  name  = "/gopherstack/appconfigdata/{{.AppName}}/{{.EnvName}}"
  type  = "String"
  value = "gopherstack"
}
