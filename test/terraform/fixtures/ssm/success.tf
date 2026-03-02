resource "aws_ssm_parameter" "this" {
  name  = "{{.ParamName}}"
  type  = "String"
  value = "test-value"
}
