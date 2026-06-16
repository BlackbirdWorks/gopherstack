data "aws_caller_identity" "current" {}

resource "aws_quicksight_group" "this" {
  group_name     = "{{.GroupName}}"
  aws_account_id = data.aws_caller_identity.current.account_id
  namespace      = "default"
  description    = "Terraform test group"
}
