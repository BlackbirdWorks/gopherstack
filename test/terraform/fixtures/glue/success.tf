resource "aws_glue_catalog_database" "this" {
  name        = "tf-glue-{{.Suffix}}"
  description = "Terraform Glue test database"
}
