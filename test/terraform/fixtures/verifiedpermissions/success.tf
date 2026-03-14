resource "terraform_data" "verifiedpermissions_policy_store" {
  triggers_replace = {
    endpoint = "{{.Endpoint}}"
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = "test"
      AWS_SECRET_ACCESS_KEY = "test"
      AWS_DEFAULT_REGION    = "us-east-1"
    }
    command = "aws --endpoint-url '{{.Endpoint}}' verifiedpermissions create-policy-store --validation-settings '{\"mode\":\"OFF\"}'"
  }
}
