resource "aws_codeconnections_connection" "this" {
  name          = "tf-conn-{{.Suffix}}"
  provider_type = "GitHub"

  tags = {
    Environment = "test"
  }
}
