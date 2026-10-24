package sdkwire

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	sdkacm "github.com/aws/aws-sdk-go-v2/service/acm"
	sdkapigateway "github.com/aws/aws-sdk-go-v2/service/apigateway"
	sdkapigatewayv2 "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	sdkappconfig "github.com/aws/aws-sdk-go-v2/service/appconfig"
	sdkathena "github.com/aws/aws-sdk-go-v2/service/athena"
	sdkbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	sdkbedrock "github.com/aws/aws-sdk-go-v2/service/bedrock"
	sdkbedrockruntime "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	sdkcloudwatch "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	sdkcloudwatchlogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	sdkcodebuild "github.com/aws/aws-sdk-go-v2/service/codebuild"
	sdkcognitoidentityprovider "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	sdkcomprehend "github.com/aws/aws-sdk-go-v2/service/comprehend"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdkecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	sdkecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	sdkeks "github.com/aws/aws-sdk-go-v2/service/eks"
	sdkemr "github.com/aws/aws-sdk-go-v2/service/emr"
	sdkeventbridge "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	sdkfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	sdkglue "github.com/aws/aws-sdk-go-v2/service/glue"
	sdkkafka "github.com/aws/aws-sdk-go-v2/service/kafka"
	sdkkinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	sdkkms "github.com/aws/aws-sdk-go-v2/service/kms"
	sdklambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	sdkmq "github.com/aws/aws-sdk-go-v2/service/mq"
	sdkopensearch "github.com/aws/aws-sdk-go-v2/service/opensearch"
	sdkorganizations "github.com/aws/aws-sdk-go-v2/service/organizations"
	sdkpipes "github.com/aws/aws-sdk-go-v2/service/pipes"
	sdksagemaker "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	sdksecretsmanager "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sdksesv2 "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sdksfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sdksqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sdktextract "github.com/aws/aws-sdk-go-v2/service/textract"
	sdktranscribe "github.com/aws/aws-sdk-go-v2/service/transcribe"
	sdktranslate "github.com/aws/aws-sdk-go-v2/service/translate"
)

// sdkClients builds the real SDK client for each registered service; the parity tests compare against them.
func sdkClients() map[string]func(aws.Config) any {
	return map[string]func(aws.Config) any{
		"acm":                     func(c aws.Config) any { return sdkacm.NewFromConfig(c) },
		"apigateway":              func(c aws.Config) any { return sdkapigateway.NewFromConfig(c) },
		"apigatewayv2":            func(c aws.Config) any { return sdkapigatewayv2.NewFromConfig(c) },
		"appconfig":               func(c aws.Config) any { return sdkappconfig.NewFromConfig(c) },
		"athena":                  func(c aws.Config) any { return sdkathena.NewFromConfig(c) },
		"batch":                   func(c aws.Config) any { return sdkbatch.NewFromConfig(c) },
		"bedrock":                 func(c aws.Config) any { return sdkbedrock.NewFromConfig(c) },
		"bedrockruntime":          func(c aws.Config) any { return sdkbedrockruntime.NewFromConfig(c) },
		"cloudwatch":              func(c aws.Config) any { return sdkcloudwatch.NewFromConfig(c) },
		"cloudwatchlogs":          func(c aws.Config) any { return sdkcloudwatchlogs.NewFromConfig(c) },
		"codebuild":               func(c aws.Config) any { return sdkcodebuild.NewFromConfig(c) },
		"cognitoidentityprovider": func(c aws.Config) any { return sdkcognitoidentityprovider.NewFromConfig(c) },
		"comprehend":              func(c aws.Config) any { return sdkcomprehend.NewFromConfig(c) },
		"dynamodb":                func(c aws.Config) any { return sdkdynamodb.NewFromConfig(c) },
		"ecr":                     func(c aws.Config) any { return sdkecr.NewFromConfig(c) },
		"ecs":                     func(c aws.Config) any { return sdkecs.NewFromConfig(c) },
		"eks":                     func(c aws.Config) any { return sdkeks.NewFromConfig(c) },
		"emr":                     func(c aws.Config) any { return sdkemr.NewFromConfig(c) },
		"eventbridge":             func(c aws.Config) any { return sdkeventbridge.NewFromConfig(c) },
		"firehose":                func(c aws.Config) any { return sdkfirehose.NewFromConfig(c) },
		"glue":                    func(c aws.Config) any { return sdkglue.NewFromConfig(c) },
		"kafka":                   func(c aws.Config) any { return sdkkafka.NewFromConfig(c) },
		"kinesis":                 func(c aws.Config) any { return sdkkinesis.NewFromConfig(c) },
		"kms":                     func(c aws.Config) any { return sdkkms.NewFromConfig(c) },
		"lambda":                  func(c aws.Config) any { return sdklambda.NewFromConfig(c) },
		"mq":                      func(c aws.Config) any { return sdkmq.NewFromConfig(c) },
		"opensearch":              func(c aws.Config) any { return sdkopensearch.NewFromConfig(c) },
		"organizations":           func(c aws.Config) any { return sdkorganizations.NewFromConfig(c) },
		"pipes":                   func(c aws.Config) any { return sdkpipes.NewFromConfig(c) },
		"sagemaker":               func(c aws.Config) any { return sdksagemaker.NewFromConfig(c) },
		"secretsmanager":          func(c aws.Config) any { return sdksecretsmanager.NewFromConfig(c) },
		"sesv2":                   func(c aws.Config) any { return sdksesv2.NewFromConfig(c) },
		"sfn":                     func(c aws.Config) any { return sdksfn.NewFromConfig(c) },
		"sqs":                     func(c aws.Config) any { return sdksqs.NewFromConfig(c) },
		"textract":                func(c aws.Config) any { return sdktextract.NewFromConfig(c) },
		"transcribe":              func(c aws.Config) any { return sdktranscribe.NewFromConfig(c) },
		"translate":               func(c aws.Config) any { return sdktranslate.NewFromConfig(c) },
	}
}
