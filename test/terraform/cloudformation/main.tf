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
    s3             = var.gopherstack_endpoint
    ec2            = var.gopherstack_endpoint
    iam            = var.gopherstack_endpoint
    elbv2          = var.gopherstack_endpoint
    dynamodb       = var.gopherstack_endpoint
    sqs            = var.gopherstack_endpoint
    sns            = var.gopherstack_endpoint
    rds            = var.gopherstack_endpoint
  }
}

variable "gopherstack_endpoint" {
  description = "Gopherstack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# ---------------------------------------------------------------------------
# CloudFormation stack deploying multiple resource types
# ---------------------------------------------------------------------------

resource "aws_cloudformation_stack" "multi_resource" {
  name = "gopherstack-cfn-test"

  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Description              = "Gopherstack CloudFormation multi-resource test stack"

    Resources = {
      # S3 bucket
      TestBucket = {
        Type = "AWS::S3::Bucket"
        Properties = {
          BucketName = "gopherstack-cfn-test-bucket"
        }
      }

      # DynamoDB table
      TestTable = {
        Type = "AWS::DynamoDB::Table"
        Properties = {
          TableName    = "gopherstack-cfn-test-table"
          BillingMode  = "PAY_PER_REQUEST"
          AttributeDefinitions = [
            { AttributeName = "id", AttributeType = "S" }
          ]
          KeySchema = [
            { AttributeName = "id", KeyType = "HASH" }
          ]
        }
      }

      # SQS queue
      TestQueue = {
        Type = "AWS::SQS::Queue"
        Properties = {
          QueueName = "gopherstack-cfn-test-queue"
        }
      }

      # SNS topic
      TestTopic = {
        Type = "AWS::SNS::Topic"
        Properties = {
          TopicName = "gopherstack-cfn-test-topic"
        }
      }

      # VPC
      TestVPC = {
        Type = "AWS::EC2::VPC"
        Properties = {
          CidrBlock = "10.0.0.0/16"
        }
      }

      # Subnet
      TestSubnet = {
        Type = "AWS::EC2::Subnet"
        Properties = {
          VpcId            = { Ref = "TestVPC" }
          CidrBlock        = "10.0.1.0/24"
          AvailabilityZone = "us-east-1a"
        }
      }

      # Internet gateway
      TestIGW = {
        Type = "AWS::EC2::InternetGateway"
        Properties = {}
      }

      # VPC gateway attachment
      TestIGWAttach = {
        Type = "AWS::EC2::VPCGatewayAttachment"
        Properties = {
          VpcId             = { Ref = "TestVPC" }
          InternetGatewayId = { Ref = "TestIGW" }
        }
      }

      # Route table
      TestRouteTable = {
        Type = "AWS::EC2::RouteTable"
        Properties = {
          VpcId = { Ref = "TestVPC" }
        }
      }

      # Subnet route table association
      TestSubnetAssoc = {
        Type = "AWS::EC2::SubnetRouteTableAssociation"
        Properties = {
          SubnetId     = { Ref = "TestSubnet" }
          RouteTableId = { Ref = "TestRouteTable" }
        }
      }

      # Security group
      TestSG = {
        Type = "AWS::EC2::SecurityGroup"
        Properties = {
          GroupName        = "gopherstack-cfn-test-sg"
          GroupDescription = "Test security group"
          VpcId            = { Ref = "TestVPC" }
        }
      }

      # IAM role
      TestRole = {
        Type = "AWS::IAM::Role"
        Properties = {
          RoleName = "gopherstack-cfn-test-role"
          AssumeRolePolicyDocument = jsonencode({
            Version = "2012-10-17"
            Statement = [{
              Effect    = "Allow"
              Principal = { Service = "lambda.amazonaws.com" }
              Action    = "sts:AssumeRole"
            }]
          })
        }
      }

      # IAM user
      TestUser = {
        Type = "AWS::IAM::User"
        Properties = {
          UserName = "gopherstack-cfn-test-user"
        }
      }

      # IAM group
      TestGroup = {
        Type = "AWS::IAM::Group"
        Properties = {
          GroupName = "gopherstack-cfn-test-group"
        }
      }

      # Lambda function
      TestLambda = {
        Type = "AWS::Lambda::Function"
        Properties = {
          FunctionName = "gopherstack-cfn-test-fn"
          Runtime      = "python3.11"
          Handler      = "index.handler"
          Role         = { "Fn::GetAtt" = ["TestRole", "Arn"] }
          Code = {
            ZipFile = "def handler(e, c): return {}"
          }
        }
        DependsOn = ["TestRole"]
      }

      # ELBv2 load balancer
      TestLB = {
        Type = "AWS::ElasticLoadBalancingV2::LoadBalancer"
        Properties = {
          Name   = "gopherstack-cfn-test-lb"
          Scheme = "internet-facing"
          Type   = "application"
        }
      }

      # ELBv2 target group
      TestTG = {
        Type = "AWS::ElasticLoadBalancingV2::TargetGroup"
        Properties = {
          Name       = "gopherstack-cfn-test-tg"
          Protocol   = "HTTP"
          Port       = 80
          VpcId      = { Ref = "TestVPC" }
          TargetType = "instance"
        }
      }

      # ELBv2 listener
      TestListener = {
        Type = "AWS::ElasticLoadBalancingV2::Listener"
        Properties = {
          LoadBalancerArn = { Ref = "TestLB" }
          Protocol        = "HTTP"
          Port            = 80
          DefaultActions = [
            {
              Type           = "forward"
              TargetGroupArn = { Ref = "TestTG" }
            }
          ]
        }
        DependsOn = ["TestLB", "TestTG"]
      }

      # RDS DB cluster parameter group
      TestClusterPG = {
        Type = "AWS::RDS::DBClusterParameterGroup"
        Properties = {
          DBClusterParameterGroupName = "gopherstack-cfn-test-cpg"
          Family                      = "aurora-postgresql15"
          Description                 = "Test cluster parameter group"
        }
      }

      # CloudWatch alarm
      TestAlarm = {
        Type = "AWS::CloudWatch::Alarm"
        Properties = {
          AlarmName           = "gopherstack-cfn-test-alarm"
          ComparisonOperator  = "GreaterThanThreshold"
          EvaluationPeriods   = 1
          MetricName          = "CPUUtilization"
          Namespace           = "AWS/EC2"
          Period              = 60
          Statistic           = "Average"
          Threshold           = 80
        }
      }

      # CloudWatch log group
      TestLogGroup = {
        Type = "AWS::Logs::LogGroup"
        Properties = {
          LogGroupName = "/gopherstack/cfn-test"
        }
      }

      # SSM parameter
      TestParam = {
        Type = "AWS::SSM::Parameter"
        Properties = {
          Name  = "/gopherstack/cfn/test"
          Type  = "String"
          Value = "hello"
        }
      }
    }

    Outputs = {
      BucketName = {
        Value = { Ref = "TestBucket" }
      }
      QueueURL = {
        Value = { Ref = "TestQueue" }
      }
      VPCID = {
        Value = { Ref = "TestVPC" }
      }
      LBArn = {
        Value = { Ref = "TestLB" }
      }
    }
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "stack_id" {
  value = aws_cloudformation_stack.multi_resource.id
}

output "stack_outputs" {
  value = aws_cloudformation_stack.multi_resource.outputs
}
