resource "terraform_data" "textract_start_detection" {
  triggers_replace = {
    endpoint = "{{.Endpoint}}"
    bucket   = "{{.Bucket}}"
    key      = "{{.Key}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' textract start-document-text-detection --document-location '{\"S3Object\":{\"Bucket\":\"{{.Bucket}}\",\"Name\":\"{{.Key}}\"}}'"
  }
}
