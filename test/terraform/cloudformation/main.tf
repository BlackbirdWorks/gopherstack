terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    cloudformation = var.gopherstack_endpoint
    ec2            = var.gopherstack_endpoint
    iam            = var.gopherstack_endpoint
    dynamodb       = var.gopherstack_endpoint
    sns            = var.gopherstack_endpoint
    sqs            = var.gopherstack_endpoint
    s3             = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# CloudFormation stack exercising new resource types added in issue #1564:
#   - AWS::EC2::Instance
#   - AWS::EC2::VPCGatewayAttachment
#   - AWS::EC2::SubnetRouteTableAssociation
#   - AWS::IAM::User
#   - AWS::IAM::Group
#   - AWS::DynamoDB::Table  (via CFN)
#   - AWS::SNS::Topic       (via CFN)
#   - AWS::SNS::Subscription (via CFN)
#   - AWS::SQS::Queue       (via CFN)
# ---------------------------------------------------------------------------

resource "aws_cloudformation_stack" "cfn_resources_extended" {
  name = "cfn-resources-extended-test"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "Gopherstack CloudFormation extended resource type coverage test"

    Resources = {
      # VPC networking
      TestVPC = {
        Type = "AWS::EC2::VPC"
        Properties = {
          CidrBlock = "10.0.0.0/16"
        }
      }

      TestSubnet = {
        Type = "AWS::EC2::Subnet"
        Properties = {
          VpcId     = { Ref = "TestVPC" }
          CidrBlock = "10.0.1.0/24"
        }
      }

      TestIGW = {
        Type = "AWS::EC2::InternetGateway"
      }

      TestVPCGatewayAttachment = {
        Type = "AWS::EC2::VPCGatewayAttachment"
        Properties = {
          VpcId             = { Ref = "TestVPC" }
          InternetGatewayId = { Ref = "TestIGW" }
        }
      }

      TestRouteTable = {
        Type = "AWS::EC2::RouteTable"
        Properties = {
          VpcId = { Ref = "TestVPC" }
        }
      }

      TestSubnetRouteTableAssociation = {
        Type = "AWS::EC2::SubnetRouteTableAssociation"
        Properties = {
          SubnetId     = { Ref = "TestSubnet" }
          RouteTableId = { Ref = "TestRouteTable" }
        }
      }

      # EC2 instance
      TestInstance = {
        Type = "AWS::EC2::Instance"
        Properties = {
          ImageId      = "ami-00000000"
          InstanceType = "t3.micro"
          SubnetId     = { Ref = "TestSubnet" }
        }
      }

      # IAM
      TestIAMUser = {
        Type = "AWS::IAM::User"
        Properties = {
          UserName = "cfn-test-user"
          Path     = "/"
        }
      }

      TestIAMGroup = {
        Type = "AWS::IAM::Group"
        Properties = {
          GroupName = "cfn-test-group"
          Path      = "/"
        }
      }

      # DynamoDB
      TestTable = {
        Type = "AWS::DynamoDB::Table"
        Properties = {
          TableName   = "cfn-test-table"
          BillingMode = "PAY_PER_REQUEST"
          AttributeDefinitions = [
            {
              AttributeName = "id"
              AttributeType = "S"
            }
          ]
          KeySchema = [
            {
              AttributeName = "id"
              KeyType       = "HASH"
            }
          ]
        }
      }

      # SNS
      TestTopic = {
        Type = "AWS::SNS::Topic"
        Properties = {
          TopicName = "cfn-test-topic"
        }
      }

      TestSubscription = {
        Type = "AWS::SNS::Subscription"
        Properties = {
          TopicArn = { Ref = "TestTopic" }
          Protocol = "sqs"
          Endpoint = { "Fn::GetAtt" = ["TestQueue", "Arn"] }
        }
      }

      # SQS
      TestQueue = {
        Type = "AWS::SQS::Queue"
        Properties = {
          QueueName = "cfn-test-queue"
        }
      }
    }

    Outputs = {
      InstanceId = {
        Value       = { Ref = "TestInstance" }
        Description = "EC2 instance ID created via CFN"
      }
      IAMUserArn = {
        Value       = { Ref = "TestIAMUser" }
        Description = "IAM user ARN created via CFN"
      }
      IAMGroupArn = {
        Value       = { Ref = "TestIAMGroup" }
        Description = "IAM group ARN created via CFN"
      }
      TableName = {
        Value       = { Ref = "TestTable" }
        Description = "DynamoDB table name created via CFN"
      }
      TopicArn = {
        Value       = { Ref = "TestTopic" }
        Description = "SNS topic ARN created via CFN"
      }
      QueueUrl = {
        Value       = { Ref = "TestQueue" }
        Description = "SQS queue URL created via CFN"
      }
    }
  })
}
