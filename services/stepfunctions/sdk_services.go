package stepfunctions

import (
	"maps"

	"github.com/aws/aws-sdk-go-v2/aws"
	xacm "github.com/aws/aws-sdk-go-v2/service/acm"
	xapigateway "github.com/aws/aws-sdk-go-v2/service/apigateway"
	xapigatewayv2 "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	xappconfig "github.com/aws/aws-sdk-go-v2/service/appconfig"
	xathena "github.com/aws/aws-sdk-go-v2/service/athena"
	xautoscaling "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	xbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	xbedrock "github.com/aws/aws-sdk-go-v2/service/bedrock"
	xbedrockruntime "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	xcloudformation "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	xcloudwatch "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	xcloudwatchlogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	xcodebuild "github.com/aws/aws-sdk-go-v2/service/codebuild"
	xcognitoidentityprovider "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	xcomprehend "github.com/aws/aws-sdk-go-v2/service/comprehend"
	xdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	xec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	xecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	xecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	xefs "github.com/aws/aws-sdk-go-v2/service/efs"
	xeks "github.com/aws/aws-sdk-go-v2/service/eks"
	xelasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	xelasticloadbalancingv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	xemr "github.com/aws/aws-sdk-go-v2/service/emr"
	xeventbridge "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	xfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	xglue "github.com/aws/aws-sdk-go-v2/service/glue"
	xiam "github.com/aws/aws-sdk-go-v2/service/iam"
	xkafka "github.com/aws/aws-sdk-go-v2/service/kafka"
	xkinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	xkms "github.com/aws/aws-sdk-go-v2/service/kms"
	xlambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	xmq "github.com/aws/aws-sdk-go-v2/service/mq"
	xopensearch "github.com/aws/aws-sdk-go-v2/service/opensearch"
	xorganizations "github.com/aws/aws-sdk-go-v2/service/organizations"
	xpipes "github.com/aws/aws-sdk-go-v2/service/pipes"
	xrds "github.com/aws/aws-sdk-go-v2/service/rds"
	xredshift "github.com/aws/aws-sdk-go-v2/service/redshift"
	xredshiftdata "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	xrekognition "github.com/aws/aws-sdk-go-v2/service/rekognition"
	xroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	xs3 "github.com/aws/aws-sdk-go-v2/service/s3"
	xsagemaker "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	xscheduler "github.com/aws/aws-sdk-go-v2/service/scheduler"
	xsecretsmanager "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	xses "github.com/aws/aws-sdk-go-v2/service/ses"
	xsesv2 "github.com/aws/aws-sdk-go-v2/service/sesv2"
	xsfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	xsns "github.com/aws/aws-sdk-go-v2/service/sns"
	xsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	xssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	xsts "github.com/aws/aws-sdk-go-v2/service/sts"
	xtextract "github.com/aws/aws-sdk-go-v2/service/textract"
	xtranscribe "github.com/aws/aws-sdk-go-v2/service/transcribe"
	xtranslate "github.com/aws/aws-sdk-go-v2/service/translate"
)

func sdkServices1() map[string]sdkService {
	return map[string]sdkService{
		"dynamodb": {prefix: "DynamoDb", build: func(c aws.Config) any {
			return xdynamodb.NewFromConfig(c)
		}},
		"sqs": {prefix: "Sqs", build: func(c aws.Config) any {
			return xsqs.NewFromConfig(c)
		}},
		"sns": {prefix: "Sns", build: func(c aws.Config) any {
			return xsns.NewFromConfig(c)
		}},
		awsServiceLambda: {prefix: "Lambda", build: func(c aws.Config) any {
			return xlambda.NewFromConfig(c)
		}},
		"secretsmanager": {prefix: "SecretsManager", build: func(c aws.Config) any {
			return xsecretsmanager.NewFromConfig(c)
		}},
		"ssm": {prefix: "Ssm", build: func(c aws.Config) any {
			return xssm.NewFromConfig(c)
		}},
		"kms": {prefix: "Kms", build: func(c aws.Config) any {
			return xkms.NewFromConfig(c)
		}},
		"eventbridge": {prefix: "EventBridge", build: func(c aws.Config) any {
			return xeventbridge.NewFromConfig(c)
		}},
		"sfn": {prefix: "Sfn", build: func(c aws.Config) any {
			return xsfn.NewFromConfig(c)
		}},
		"ecs": {prefix: "Ecs", build: func(c aws.Config) any {
			return xecs.NewFromConfig(c)
		}},
		"glue": {prefix: "Glue", build: func(c aws.Config) any {
			return xglue.NewFromConfig(c)
		}},
		"athena": {prefix: "Athena", build: func(c aws.Config) any {
			return xathena.NewFromConfig(c)
		}},
		"batch": {prefix: "Batch", build: func(c aws.Config) any {
			return xbatch.NewFromConfig(c)
		}},
		"sts": {prefix: "Sts", build: func(c aws.Config) any {
			return xsts.NewFromConfig(c)
		}},
		"iam": {prefix: "Iam", build: func(c aws.Config) any {
			return xiam.NewFromConfig(c)
		}},
		"kinesis": {prefix: "Kinesis", build: func(c aws.Config) any {
			return xkinesis.NewFromConfig(c)
		}},
		"firehose": {prefix: "Firehose", build: func(c aws.Config) any {
			return xfirehose.NewFromConfig(c)
		}},
		"cloudwatch": {prefix: "CloudWatch", build: func(c aws.Config) any {
			return xcloudwatch.NewFromConfig(c)
		}},
	}
}

func sdkServices2() map[string]sdkService {
	return map[string]sdkService{
		"cloudwatchlogs": {prefix: "CloudWatchLogs", build: func(c aws.Config) any {
			return xcloudwatchlogs.NewFromConfig(c)
		}},
		"ec2": {prefix: "Ec2", build: func(c aws.Config) any {
			return xec2.NewFromConfig(c)
		}},
		"ecr": {prefix: "Ecr", build: func(c aws.Config) any {
			return xecr.NewFromConfig(c)
		}},
		"route53": {prefix: "Route53", build: func(c aws.Config) any {
			return xroute53.NewFromConfig(c)
		}},
		"cognitoidentityprovider": {prefix: "CognitoIdentityProvider", build: func(c aws.Config) any {
			return xcognitoidentityprovider.NewFromConfig(c)
		}},
		"apigateway": {prefix: "ApiGateway", build: func(c aws.Config) any {
			return xapigateway.NewFromConfig(c)
		}},
		"apigatewayv2": {prefix: "ApiGatewayV2", build: func(c aws.Config) any {
			return xapigatewayv2.NewFromConfig(c)
		}},
		"codebuild": {prefix: "CodeBuild", build: func(c aws.Config) any {
			return xcodebuild.NewFromConfig(c)
		}},
		"bedrockruntime": {prefix: "BedrockRuntime", build: func(c aws.Config) any {
			return xbedrockruntime.NewFromConfig(c)
		}},
		"bedrock": {prefix: "Bedrock", build: func(c aws.Config) any {
			return xbedrock.NewFromConfig(c)
		}},
		"sagemaker": {prefix: "SageMaker", build: func(c aws.Config) any {
			return xsagemaker.NewFromConfig(c)
		}},
		"emr": {prefix: "Emr", build: func(c aws.Config) any {
			return xemr.NewFromConfig(c)
		}},
		"eks": {prefix: "Eks", build: func(c aws.Config) any {
			return xeks.NewFromConfig(c)
		}},
		"ses": {prefix: "Ses", build: func(c aws.Config) any {
			return xses.NewFromConfig(c)
		}},
		"sesv2": {prefix: "SesV2", build: func(c aws.Config) any {
			return xsesv2.NewFromConfig(c)
		}},
		"acm": {prefix: "Acm", build: func(c aws.Config) any {
			return xacm.NewFromConfig(c)
		}},
		"elasticache": {prefix: "ElastiCache", build: func(c aws.Config) any {
			return xelasticache.NewFromConfig(c)
		}},
		"rds": {prefix: "Rds", build: func(c aws.Config) any {
			return xrds.NewFromConfig(c)
		}},
		"redshift": {prefix: "Redshift", build: func(c aws.Config) any {
			return xredshift.NewFromConfig(c)
		}},
	}
}

func sdkServices3() map[string]sdkService {
	return map[string]sdkService{
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
		"appconfig": {prefix: "AppConfig", build: func(c aws.Config) any {
			return xappconfig.NewFromConfig(c)
		}},
		"scheduler": {prefix: "Scheduler", build: func(c aws.Config) any {
			return xscheduler.NewFromConfig(c)
		}},
		"pipes": {prefix: "Pipes", build: func(c aws.Config) any {
			return xpipes.NewFromConfig(c)
		}},
		"organizations": {prefix: "Organizations", build: func(c aws.Config) any {
			return xorganizations.NewFromConfig(c)
		}},
		"opensearch": {prefix: "OpenSearch", build: func(c aws.Config) any {
			return xopensearch.NewFromConfig(c)
		}},
		"kafka": {prefix: "Kafka", build: func(c aws.Config) any {
			return xkafka.NewFromConfig(c)
		}},
		"mq": {prefix: "Mq", build: func(c aws.Config) any {
			return xmq.NewFromConfig(c)
		}},
		"transcribe": {prefix: "Transcribe", build: func(c aws.Config) any {
			return xtranscribe.NewFromConfig(c)
		}},
		"translate": {prefix: "Translate", build: func(c aws.Config) any {
			return xtranslate.NewFromConfig(c)
		}},
		"textract": {prefix: "Textract", build: func(c aws.Config) any {
			return xtextract.NewFromConfig(c)
		}},
		"rekognition": {prefix: "Rekognition", build: func(c aws.Config) any {
			return xrekognition.NewFromConfig(c)
		}},
		"comprehend": {prefix: "Comprehend", build: func(c aws.Config) any {
			return xcomprehend.NewFromConfig(c)
		}},
	}
}

// sdkServiceTable lists the AWS SDK integrations (aws-sdk:<service>:<action>)
// served in-process; prefixes are the documented error-name prefixes.
func sdkServiceTable() map[string]sdkService {
	tbl := map[string]sdkService{
		"s3": {prefix: "S3", build: func(c aws.Config) any {
			return xs3.NewFromConfig(c, func(o *xs3.Options) { o.UsePathStyle = true })
		}},
	}

	for _, part := range []map[string]sdkService{sdkServices1(), sdkServices2(), sdkServices3()} {
		maps.Copy(tbl, part)
	}

	return tbl
}
