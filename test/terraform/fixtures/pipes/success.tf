resource "aws_iam_role" "pipe" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pipes.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_sqs_queue" "source" {
  name = "{{.PipeName}}-source"
}

resource "aws_sqs_queue" "target" {
  name = "{{.PipeName}}-target"
}

resource "aws_pipes_pipe" "this" {
  name     = "{{.PipeName}}"
  role_arn = aws_iam_role.pipe.arn
  source   = aws_sqs_queue.source.arn
  target   = aws_sqs_queue.target.arn
}
