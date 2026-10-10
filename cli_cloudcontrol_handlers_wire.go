package main

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/route53"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

func wireCloudControlInfraHandlers(bk *cloudcontrolbackend.InMemoryBackend, cfg aws.Config) {
	ec2c := ec2.NewFromConfig(cfg)

	bk.RegisterTypeHandler("AWS::EC2::VPC", &ccVPC{client: ec2c})
	bk.RegisterTypeHandler("AWS::EC2::Subnet", &ccSubnet{client: ec2c})
	bk.RegisterTypeHandler("AWS::EC2::SecurityGroup", &ccSecurityGroup{client: ec2c})
	bk.RegisterTypeHandler("AWS::ECS::Cluster", &ccCluster{client: ecs.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::ElasticLoadBalancingV2::TargetGroup", &ccTargetGroup{client: elbv2.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Route53::HostedZone", &ccHostedZone{client: route53.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::CloudWatch::Alarm", &ccAlarm{client: cloudwatch.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Cognito::UserPool", &ccUserPool{
		client: cognitoidentityprovider.NewFromConfig(cfg), region: bk.Region(),
	})
	bk.RegisterTypeHandler("AWS::ApiGateway::RestApi", &ccRestAPI{
		client: apigateway.NewFromConfig(cfg), region: bk.Region(),
	})
	bk.RegisterTypeHandler("AWS::EFS::FileSystem", &ccFileSystem{client: efs.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Glue::Database", &ccGlueDatabase{client: glue.NewFromConfig(cfg)})
}
