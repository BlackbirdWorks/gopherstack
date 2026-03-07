resource "aws_ecr_repository" "this" {
  name = "{{.RepoName}}"
}

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

  environment {
    variables = {
      ECR_REPO_URL = aws_ecr_repository.this.repository_url
    }
  }
}
