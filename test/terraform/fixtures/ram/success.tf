resource "aws_ram_resource_share" "this" {
  name                      = "{{.ShareName}}"
  allow_external_principals = true

  tags = {
    Environment = "test"
  }
}
