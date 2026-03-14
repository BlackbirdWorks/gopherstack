resource "terraform_data" "s3tables_bucket" {
  triggers_replace = {
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' s3tables create-table-bucket --name tf-s3t-{{.Suffix}}"
  }
}
