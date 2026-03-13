resource "terraform_data" "shield_subscription" {
  triggers_replace = {
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' shield create-subscription"
  }
}

resource "terraform_data" "shield_protection" {
  depends_on = [terraform_data.shield_subscription]

  triggers_replace = {
    name     = "{{.ProtectionName}}"
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' shield create-protection --name '{{.ProtectionName}}' --resource-arn '{{.ResourceARN}}'"
  }
}
