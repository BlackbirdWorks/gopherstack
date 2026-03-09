resource "aws_iam_role" "lambda" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "this" {
  filename         = "{{.ZipPath}}"
  function_name    = "{{.FuncName}}"
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.ZipPath}}")
  publish          = true
}

resource "aws_lambda_provisioned_concurrency_config" "this" {
  function_name                      = aws_lambda_function.this.function_name
  provisioned_concurrent_executions  = 2
  qualifier                          = aws_lambda_function.this.version
}
