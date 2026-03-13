resource "aws_qldb_ledger" "this" {
  name             = "{{.LedgerName}}"
  permissions_mode = "ALLOW_ALL"

  tags = {
    Environment = "test"
  }
}
