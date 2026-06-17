data "aws_iam_policy_document" "placeholder" {
  statement {
    actions   = ["mediatailor:*"]
    resources = ["*"]
  }
}
