resource "aws_glacier_vault" "this" {
  name = "{{.VaultName}}"

  tags = {
    Environment = "test"
  }
}
