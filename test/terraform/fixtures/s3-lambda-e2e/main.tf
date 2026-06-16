resource "aws_sqs_queue" "receipt" {
  name = "{{.QueueName}}"
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

resource "aws_iam_role_policy" "lambda_sqs" {
  name = "{{.RoleName}}-sqs"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = aws_sqs_queue.receipt.arn
    }]
  })
}

resource "aws_lambda_function" "receiver" {
  filename         = "{{.ZipPath}}"
  function_name    = "{{.FuncName}}"
  role             = aws_iam_role.lambda.arn
  handler          = "index.handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("{{.ZipPath}}")

  environment {
    variables = {
      QUEUE_URL = aws_sqs_queue.receipt.url
    }
  }
}

resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.receiver.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.trigger.arn
}

resource "aws_s3_bucket" "trigger" {
  bucket        = "{{.BucketName}}"
  force_destroy = true
}

resource "aws_s3_bucket_notification" "trigger" {
  bucket = aws_s3_bucket.trigger.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.receiver.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.s3]
}
