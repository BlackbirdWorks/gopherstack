resource "terraform_data" "s3tables_bucket" {
  triggers_replace = {
    suffix   = "{{.Suffix}}"
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

  provisioner "local-exec" {
    when = destroy
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '${self.triggers_replace.endpoint}' s3tables delete-table-bucket --table-bucket-arn arn:aws:s3tables:us-east-1:123456789012:bucket/tf-s3t-${self.triggers_replace.suffix} || true"
  }
}
