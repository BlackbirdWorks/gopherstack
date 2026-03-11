resource "aws_codestarconnections_connection" "this" {
  name          = "tf-conn-{{.Suffix}}"
  provider_type = "GitHub"
}
