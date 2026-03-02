resource "aws_cloudformation_stack" "this" {
  name = "{{.StackName}}"

  timeouts {
    create = "2m"
    delete = "2m"
  }

  template_body = <<TEMPLATE
{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Description": "Gopherstack test stack",
  "Resources": {
    "WaitHandle": {
      "Type": "AWS::CloudFormation::WaitConditionHandle"
    }
  }
}
TEMPLATE
}
