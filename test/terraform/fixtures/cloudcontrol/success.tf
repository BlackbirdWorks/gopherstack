resource "aws_cloudcontrolapi_resource" "test" {
  type_name     = "AWS::Logs::LogGroup"
  desired_state = jsonencode({
    LogGroupName = "tf-cloudcontrol-{{.Suffix}}"
  })
}
