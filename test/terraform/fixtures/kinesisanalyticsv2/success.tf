resource "aws_kinesisanalyticsv2_application" "this" {
  name                   = "tf-kinesisanalyticsv2-{{.Suffix}}"
  runtime_environment    = "FLINK-1_18"
  service_execution_role = "arn:aws:iam::000000000000:role/service-role"

  tags = {
    Environment = "test"
  }
}
