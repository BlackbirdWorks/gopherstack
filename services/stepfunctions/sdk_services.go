package stepfunctions

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	xautoscaling "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	xcloudformation "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	xec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	xefs "github.com/aws/aws-sdk-go-v2/service/efs"
	xelasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	xelasticloadbalancingv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	xiam "github.com/aws/aws-sdk-go-v2/service/iam"
	xrds "github.com/aws/aws-sdk-go-v2/service/rds"
	xredshift "github.com/aws/aws-sdk-go-v2/service/redshift"
	xredshiftdata "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	xrekognition "github.com/aws/aws-sdk-go-v2/service/rekognition"
	xroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	xs3 "github.com/aws/aws-sdk-go-v2/service/s3"
	xscheduler "github.com/aws/aws-sdk-go-v2/service/scheduler"
	xses "github.com/aws/aws-sdk-go-v2/service/ses"
	xsns "github.com/aws/aws-sdk-go-v2/service/sns"
	xssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	xsts "github.com/aws/aws-sdk-go-v2/service/sts"
)

// sdkServiceTable lists the AWS SDK integrations (aws-sdk:<service>:<action>)
// served in-process; prefixes are the documented error-name prefixes. Services
// without a build func are served by sdkwire from their Smithy schemas.
func sdkServiceTable() map[string]sdkService {
	return map[string]sdkService{
		"dynamodb": {prefix: "DynamoDb"},
		"sqs":      {prefix: "Sqs"},
		"sns": {prefix: "Sns", build: func(c aws.Config) any {
			return xsns.NewFromConfig(c)
		}},
		awsServiceLambda: {prefix: "Lambda"},
		"secretsmanager": {prefix: "SecretsManager"},
		"ssm": {prefix: "Ssm", build: func(c aws.Config) any {
			return xssm.NewFromConfig(c)
		}},
		"kms":         {prefix: "Kms"},
		"eventbridge": {prefix: "EventBridge"},
		"sfn":         {prefix: "Sfn"},
		"ecs":         {prefix: "Ecs"},
		"glue":        {prefix: "Glue"},
		"athena":      {prefix: "Athena"},
		"batch":       {prefix: "Batch"},
		"sts": {prefix: "Sts", build: func(c aws.Config) any {
			return xsts.NewFromConfig(c)
		}},
		"iam": {prefix: "Iam", build: func(c aws.Config) any {
			return xiam.NewFromConfig(c)
		}},
		"kinesis":        {prefix: "Kinesis"},
		"firehose":       {prefix: "Firehose"},
		"cloudwatch":     {prefix: "CloudWatch"},
		"cloudwatchlogs": {prefix: "CloudWatchLogs"},
		"ec2": {prefix: "Ec2", build: func(c aws.Config) any {
			return xec2.NewFromConfig(c)
		}},
		"ecr": {prefix: "Ecr"},
		"route53": {prefix: "Route53", build: func(c aws.Config) any {
			return xroute53.NewFromConfig(c)
		}},
		"cognitoidentityprovider": {prefix: "CognitoIdentityProvider"},
		"apigateway":              {prefix: "ApiGateway"},
		"apigatewayv2":            {prefix: "ApiGatewayV2"},
		"codebuild":               {prefix: "CodeBuild"},
		svcBedrockRuntime:         {prefix: "BedrockRuntime"},
		"bedrock":                 {prefix: "Bedrock"},
		"sagemaker":               {prefix: "SageMaker"},
		"emr":                     {prefix: "Emr"},
		"eks":                     {prefix: "Eks"},
		"ses": {prefix: "Ses", build: func(c aws.Config) any {
			return xses.NewFromConfig(c)
		}},
		"sesv2": {prefix: "SesV2"},
		"acm":   {prefix: "Acm"},
		"elasticache": {prefix: "ElastiCache", build: func(c aws.Config) any {
			return xelasticache.NewFromConfig(c)
		}},
		"rds": {prefix: "Rds", build: func(c aws.Config) any {
			return xrds.NewFromConfig(c)
		}},
		"redshift": {prefix: "Redshift", build: func(c aws.Config) any {
			return xredshift.NewFromConfig(c)
		}},
		"redshiftdata": {prefix: "RedshiftData", build: func(c aws.Config) any {
			return xredshiftdata.NewFromConfig(c)
		}},
		"cloudformation": {prefix: "CloudFormation", build: func(c aws.Config) any {
			return xcloudformation.NewFromConfig(c)
		}},
		"autoscaling": {prefix: "AutoScaling", build: func(c aws.Config) any {
			return xautoscaling.NewFromConfig(c)
		}},
		"elasticloadbalancingv2": {prefix: "ElasticLoadBalancingV2", build: func(c aws.Config) any {
			return xelasticloadbalancingv2.NewFromConfig(c)
		}},
		"efs": {prefix: "Efs", build: func(c aws.Config) any {
			return xefs.NewFromConfig(c)
		}},
		"appconfig": {prefix: "AppConfig"},
		"scheduler": {prefix: "Scheduler", build: func(c aws.Config) any {
			return xscheduler.NewFromConfig(c)
		}},
		"pipes":         {prefix: "Pipes"},
		"organizations": {prefix: "Organizations"},
		"opensearch":    {prefix: "OpenSearch"},
		"kafka":         {prefix: "Kafka"},
		"mq":            {prefix: "Mq"},
		"transcribe":    {prefix: "Transcribe"},
		"translate":     {prefix: "Translate"},
		"textract":      {prefix: "Textract"},
		"rekognition": {prefix: "Rekognition", build: func(c aws.Config) any {
			return xrekognition.NewFromConfig(c)
		}},
		"comprehend": {prefix: "Comprehend"},
		"s3": {prefix: "S3", build: func(c aws.Config) any {
			return xs3.NewFromConfig(c, func(o *xs3.Options) { o.UsePathStyle = true })
		}},
	}
}
