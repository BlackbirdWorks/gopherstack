resource "aws_lambda_function" "this" {
  function_name = "{{.FuncName}}"
  role          = "{{.RoleARN}}"
  handler       = "index.handler"
  runtime       = "python3.12"
  filename      = "{{.ZipPath}}"
}
