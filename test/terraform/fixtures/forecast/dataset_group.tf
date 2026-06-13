data "aws_iam_policy_document" "placeholder" {
  statement {
    actions   = ["forecast:*"]
    resources = ["*"]
  }
}
