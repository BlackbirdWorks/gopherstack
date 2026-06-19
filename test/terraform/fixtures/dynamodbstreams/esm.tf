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
}

resource "aws_dynamodb_table" "this" {
  name             = "{{.TableName}}"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "pk"
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  attribute {
    name = "pk"
    type = "S"
  }
}

resource "aws_lambda_event_source_mapping" "this" {
  event_source_arn  = aws_dynamodb_table.this.stream_arn
  function_name     = aws_lambda_function.this.arn
  starting_position = "TRIM_HORIZON"
  batch_size        = 10

  depends_on = [aws_dynamodb_table.this, aws_lambda_function.this]
}
