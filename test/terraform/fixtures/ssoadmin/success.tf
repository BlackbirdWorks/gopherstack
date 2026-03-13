resource "terraform_data" "ssoadmin_instance" {
  triggers_replace = {
    name     = "{{.InstanceName}}"
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' sso-admin create-instance --name '{{.InstanceName}}' --output json > /tmp/ssoadmin_instance.json && cat /tmp/ssoadmin_instance.json"
  }
}
