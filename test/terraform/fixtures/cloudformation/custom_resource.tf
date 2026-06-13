resource "aws_cloudformation_stack" "extensibility" {
  name = "{{.StackName}}"

  timeouts {
    create = "5m"
    delete = "5m"
  }

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "Gopherstack CFN extensibility round-trip: CustomResource + WaitCondition + Macro"

    Resources = {
      WaitHandle = {
        Type = "AWS::CloudFormation::WaitConditionHandle"
      }

      WaitCond = {
        Type      = "AWS::CloudFormation::WaitCondition"
        DependsOn = ["WaitHandle"]
        Properties = {
          Handle  = { Ref = "WaitHandle" }
          Count   = "1"
          Timeout = "60"
        }
      }

      CustomWidget = {
        Type = "Custom::Widget"
        Properties = {
          ServiceToken = "arn:aws:lambda:us-east-1:000000000000:function:absent-custom-resource-fn"
          Color        = "blue"
          Size         = "large"
        }
      }

      TransformMacro = {
        Type = "AWS::CloudFormation::Macro"
        Properties = {
          Name         = "{{.MacroName}}"
          FunctionName = "arn:aws:lambda:us-east-1:000000000000:function:transform-macro-fn"
          Description  = "Test macro for gopherstack extensibility parity"
        }
      }
    }

    Outputs = {
      StackName = {
        Value       = { Ref = "AWS::StackName" }
        Description = "The stack name"
      }
      MacroName = {
        Value       = { Ref = "TransformMacro" }
        Description = "The registered macro name"
      }
    }
  })
}
