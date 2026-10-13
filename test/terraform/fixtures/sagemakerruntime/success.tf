resource "aws_sagemaker_model" "runtime" {
  name               = "tf-test-runtime-model"
  execution_role_arn = "arn:aws:iam::000000000000:role/sagemaker-role"

  primary_container {
    image = "000000000000.dkr.ecr.us-east-1.amazonaws.com/myimage:latest"
  }
}

resource "aws_sagemaker_endpoint_configuration" "runtime" {
  name = "tf-test-runtime-endpoint-config"

  production_variants {
    variant_name           = "AllTraffic"
    model_name             = aws_sagemaker_model.runtime.name
    initial_instance_count = 1
    instance_type          = "ml.t2.medium"
  }
}

# aws_sagemaker_endpoint waits for the endpoint to reach InService.
resource "aws_sagemaker_endpoint" "runtime" {
  name                 = "tf-test-endpoint"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.runtime.name
}
