package app

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/route53"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

func wireCloudControlInfraHandlers(
	bk *cloudcontrolbackend.InMemoryBackend,
	e http.Handler,
	cfg aws.Config,
	ec2c *ccEC2Client,
) {
	bk.RegisterTypeHandler("AWS::EC2::VPC", &ccVPC{client: ec2c})
	bk.RegisterTypeHandler("AWS::EC2::Subnet", &ccSubnet{client: ec2c})
	bk.RegisterTypeHandler("AWS::EC2::SecurityGroup", &ccSecurityGroup{client: ec2c})
	bk.RegisterTypeHandler("AWS::Route53::HostedZone", &ccHostedZone{client: route53.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::CloudWatch::Alarm", &ccAlarm{client: cloudwatch.NewFromConfig(cfg)})

	wc := func(key string) *ccWireClient {
		c, err := newCCWireClient(key, e, cfg)
		if err != nil {
			return nil
		}

		return c
	}

	if c := wc("ecs"); c != nil {
		bk.RegisterTypeHandler("AWS::ECS::Cluster", &ccCluster{client: &wireECS{c}})
	}

	if c := wc("elasticloadbalancingv2"); c != nil {
		bk.RegisterTypeHandler("AWS::ElasticLoadBalancingV2::TargetGroup", &ccTargetGroup{client: &wireELBv2{c}})
	}

	if c := wc("ecr"); c != nil {
		bk.RegisterTypeHandler("AWS::ECR::Repository", &ccRepository{client: &wireECR{c}})
	}

	if c := wc("cognitoidentityprovider"); c != nil {
		bk.RegisterTypeHandler("AWS::Cognito::UserPool", &ccUserPool{client: &wireCognito{c}, region: bk.Region()})
	}

	if c := wc("apigateway"); c != nil {
		bk.RegisterTypeHandler("AWS::ApiGateway::RestApi", &ccRestAPI{client: &wireAPIGW{c}, region: bk.Region()})
	}

	if c := wc("efs"); c != nil {
		bk.RegisterTypeHandler("AWS::EFS::FileSystem", &ccFileSystem{client: &wireEFS{c}})
	}

	if c := wc("glue"); c != nil {
		bk.RegisterTypeHandler("AWS::Glue::Database", &ccGlueDatabase{client: &wireGlue{c}})
	}
}
