resource "terraform_data" "serverlessrepo_app" {
  triggers_replace = {
    name = "{{.ApplicationName}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' serverlessrepo create-application --name '{{.ApplicationName}}' --description 'A test serverless application' --author 'test-author' --semantic-version '1.0.0'"
  }
}
