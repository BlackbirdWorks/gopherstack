resource "aws_swf_domain" "this" {
  name                                        = "{{.DomainName}}"
  workflow_execution_retention_period_in_days = 1
}
