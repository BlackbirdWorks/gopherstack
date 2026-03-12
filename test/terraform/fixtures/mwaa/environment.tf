resource "aws_mwaa_environment" "this" {
  name               = "{{.EnvironmentName}}"
  dag_s3_path        = "dags/"
  execution_role_arn = "arn:aws:iam::123456789012:role/mwaa-role"
  source_bucket_arn  = "arn:aws:s3:::my-mwaa-bucket"

  network_configuration {
    security_group_ids = ["sg-12345678"]
    subnet_ids         = ["subnet-12345678", "subnet-87654321"]
  }

  tags = {
    Environment = "test"
  }
}
