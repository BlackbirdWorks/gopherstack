resource "aws_workmail_organization" "this" {
  alias = "{{.Alias}}"

  tags = {
    Environment = "test"
  }
}
