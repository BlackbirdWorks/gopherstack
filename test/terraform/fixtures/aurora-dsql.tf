resource "aws_dsql_cluster" "this" {
  deletion_protection_enabled = false

  tags = {
    Environment = "test"
    Owner       = "terraform"
  }
}
