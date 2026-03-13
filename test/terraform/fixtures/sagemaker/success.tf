resource "aws_sagemaker_model" "this" {
  name               = "{{.ModelName}}"
  execution_role_arn = "arn:aws:iam::000000000000:role/test-sagemaker-role"

  primary_container {
    image = "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
  }

  tags = {
    Environment = "test"
  }
}
