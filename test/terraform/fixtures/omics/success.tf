resource "aws_omics_reference_store" "this" {
  name = "{{.StoreName}}"

  tags = {
    Env = "test"
  }
}
