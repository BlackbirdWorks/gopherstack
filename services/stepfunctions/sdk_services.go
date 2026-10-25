package stepfunctions

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	xs3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// sdkServiceTable lists the AWS SDK integrations (aws-sdk:<service>:<action>)
// served in-process; prefixes are the documented error-name prefixes. Services
// without a build func are served by sdkwire from their Smithy schemas.
func sdkServiceTable() map[string]sdkService {
	return map[string]sdkService{
		"dynamodb":       {prefix: "DynamoDb"},
		"sqs":            {prefix: "Sqs"},
		"sns":            {prefix: "Sns"},
		awsServiceLambda: {prefix: "Lambda"},
		"secretsmanager": {prefix: "SecretsManager"},
		"ssm":            {prefix: "Ssm"},
		"kms":            {prefix: "Kms"},
		"eventbridge":    {prefix: "EventBridge"},
		"sfn":            {prefix: "Sfn"},
		"ecs":            {prefix: "Ecs"},
		"glue":           {prefix: "Glue"},
		"athena":         {prefix: "Athena"},
		"batch":          {prefix: "Batch"},
		"sts":            {prefix: "Sts"},
		"iam":            {prefix: "Iam"},
		"kinesis":        {prefix: "Kinesis"},
		"firehose":       {prefix: "Firehose"},
		"cloudwatch":     {prefix: "CloudWatch"},
		"cloudwatchlogs": {prefix: "CloudWatchLogs"},
		"ec2":            {prefix: "Ec2"},
		"ecr":            {prefix: "Ecr"},
		"route53":                 {prefix: "Route53"},
		"cognitoidentityprovider": {prefix: "CognitoIdentityProvider"},
		"apigateway":              {prefix: "ApiGateway"},
		"apigatewayv2":            {prefix: "ApiGatewayV2"},
		"codebuild":               {prefix: "CodeBuild"},
		svcBedrockRuntime:         {prefix: "BedrockRuntime"},
		"bedrock":                 {prefix: "Bedrock"},
		"sagemaker":               {prefix: "SageMaker"},
		"emr":                     {prefix: "Emr"},
		"eks":                     {prefix: "Eks"},
		"ses":                     {prefix: "Ses"},
		"sesv2":                   {prefix: "SesV2"},
		"acm":                     {prefix: "Acm"},
		"elasticache":             {prefix: "ElastiCache"},
		"rds":                     {prefix: "Rds"},
		"redshift":                {prefix: "Redshift"},
		"redshiftdata":            {prefix: "RedshiftData"},
		"cloudformation":          {prefix: "CloudFormation"},
		"autoscaling":             {prefix: "AutoScaling"},
		"elasticloadbalancingv2":  {prefix: "ElasticLoadBalancingV2"},
		"efs":                     {prefix: "Efs"},
		"appconfig":               {prefix: "AppConfig"},
		"scheduler":               {prefix: "Scheduler"},
		"pipes":                   {prefix: "Pipes"},
		"organizations":           {prefix: "Organizations"},
		"opensearch":              {prefix: "OpenSearch"},
		"kafka":                   {prefix: "Kafka"},
		"mq":                      {prefix: "Mq"},
		"transcribe":              {prefix: "Transcribe"},
		"translate":               {prefix: "Translate"},
		"textract":                {prefix: "Textract"},
		"rekognition":             {prefix: "Rekognition"},
		"comprehend":              {prefix: "Comprehend"},
		"s3": {prefix: "S3", build: func(c aws.Config) any {
			return xs3.NewFromConfig(c, func(o *xs3.Options) { o.UsePathStyle = true })
		}},
	}
}
