resource "aws_backup_vault" "this" {
  name = "{{.VaultName}}"

  tags = {
    Environment = "test"
  }
}
