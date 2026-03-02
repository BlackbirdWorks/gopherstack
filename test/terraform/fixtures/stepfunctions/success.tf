resource "aws_iam_role" "sfn" {
  name = "{{.RoleName}}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_sfn_state_machine" "this" {
  name     = "{{.SMName}}"
  role_arn = aws_iam_role.sfn.arn
  definition = jsonencode({
    Comment = "test"
    StartAt = "Pass"
    States  = {
      Pass = { Type = "Pass", End = true }
    }
  })
}
