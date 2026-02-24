package dashboard

import (
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	cwbackend "github.com/blackbirdworks/gopherstack/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	globalcfg "github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	"github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	"github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

// AWSSDKProvider is a private interface to extract AWS SDK clients
// from the abstract AppContext Config.
type AWSSDKProvider interface {
	GetDynamoDBClient() *ddbsdk.Client
	GetS3Client() *s3sdk.Client
	GetSSMClient() *ssmsdk.Client
	GetSTSClient() *stssdk.Client
	GetDynamoDBHandler() service.Registerable
	GetS3Handler() service.Registerable
	GetSSMHandler() service.Registerable
	GetIAMHandler() service.Registerable
	GetSTSHandler() service.Registerable
	GetSNSHandler() service.Registerable
	GetSQSHandler() service.Registerable
	GetKMSHandler() service.Registerable
	GetSecretsManagerHandler() service.Registerable
	GetLambdaHandler() service.Registerable
	GetEventBridgeHandler() service.Registerable
	GetAPIGatewayHandler() service.Registerable
	GetCloudWatchLogsHandler() service.Registerable
	GetStepFunctionsHandler() service.Registerable
	GetCloudWatchHandler() service.Registerable
	GetGlobalConfig() globalcfg.GlobalConfig
}

// Provider implements service.Provider for the Dashboard service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "Dashboard"
}

// Init initializes the Dashboard service.
//
// extractedConfig holds all concrete service types extracted from a AWSSDKProvider.
type extractedConfig struct {
	ddbClient         *ddbsdk.Client
	s3Client          *s3sdk.Client
	ssmClient         *ssmsdk.Client
	ddb               *dynamodb.DynamoDBHandler
	s3h               *s3.S3Handler
	ssmOps            *ssm.Handler
	iamOps            *iambackend.Handler
	stsOps            *stsbackend.Handler
	snsOps            *sns.Handler
	sqsOps            *sqsbackend.Handler
	kmsOps            *kmsbackend.Handler
	secretsManagerOps *secretsmanagerbackend.Handler
	lambdaOps         *lambdabackend.Handler
	eventBridgeOps    *ebbackend.Handler
	apiGatewayOps     *apigwbackend.Handler
	cloudWatchLogsOps *cwlogsbackend.Handler
	stepFunctionsOps  *sfnbackend.Handler
	cloudWatchOps     *cwbackend.Handler
	gCfg              globalcfg.GlobalConfig
}

// extractFromProvider tries to extract all service types from the AppContext.Config.
func extractFromProvider(ctx *service.AppContext) extractedConfig {
	var ec extractedConfig

	ap, ok := ctx.Config.(AWSSDKProvider)
	if !ok {
		return ec
	}

	ec.ddbClient = ap.GetDynamoDBClient()
	ec.s3Client = ap.GetS3Client()
	ec.ssmClient = ap.GetSSMClient()
	ec.gCfg = ap.GetGlobalConfig()
	ec.ddb, _ = ap.GetDynamoDBHandler().(*dynamodb.DynamoDBHandler)
	ec.s3h, _ = ap.GetS3Handler().(*s3.S3Handler)

	if h := ap.GetSSMHandler(); h != nil {
		ec.ssmOps, _ = h.(*ssm.Handler)
	}

	if h := ap.GetIAMHandler(); h != nil {
		ec.iamOps, _ = h.(*iambackend.Handler)
	}

	if h := ap.GetSTSHandler(); h != nil {
		ec.stsOps, _ = h.(*stsbackend.Handler)
	}

	if h := ap.GetSNSHandler(); h != nil {
		ec.snsOps, _ = h.(*sns.Handler)
	}

	if h := ap.GetSQSHandler(); h != nil {
		ec.sqsOps, _ = h.(*sqsbackend.Handler)
	}

	if h := ap.GetKMSHandler(); h != nil {
		ec.kmsOps, _ = h.(*kmsbackend.Handler)
	}

	if h := ap.GetSecretsManagerHandler(); h != nil {
		ec.secretsManagerOps, _ = h.(*secretsmanagerbackend.Handler)
	}

	if h := ap.GetLambdaHandler(); h != nil {
		ec.lambdaOps, _ = h.(*lambdabackend.Handler)
	}

	if h := ap.GetEventBridgeHandler(); h != nil {
		ec.eventBridgeOps, _ = h.(*ebbackend.Handler)
	}

	if h := ap.GetAPIGatewayHandler(); h != nil {
		ec.apiGatewayOps, _ = h.(*apigwbackend.Handler)
	}

	if h := ap.GetCloudWatchLogsHandler(); h != nil {
		ec.cloudWatchLogsOps, _ = h.(*cwlogsbackend.Handler)
	}

	if h := ap.GetStepFunctionsHandler(); h != nil {
		ec.stepFunctionsOps, _ = h.(*sfnbackend.Handler)
	}

	if h := ap.GetCloudWatchHandler(); h != nil {
		ec.cloudWatchOps, _ = h.(*cwbackend.Handler)
	}

	return ec
}

//nolint:ireturn // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	ec := extractFromProvider(ctx)

	handler := NewHandler(Config{
		DDBClient:         ec.ddbClient,
		S3Client:          ec.s3Client,
		SSMClient:         ec.ssmClient,
		DDBOps:            ec.ddb,
		S3Ops:             ec.s3h,
		SSMOps:            ec.ssmOps,
		IAMOps:            ec.iamOps,
		STSOps:            ec.stsOps,
		SNSOps:            ec.snsOps,
		SQSOps:            ec.sqsOps,
		KMSOps:            ec.kmsOps,
		SecretsManagerOps: ec.secretsManagerOps,
		LambdaOps:         ec.lambdaOps,
		EventBridgeOps:    ec.eventBridgeOps,
		APIGatewayOps:     ec.apiGatewayOps,
		CloudWatchLogsOps: ec.cloudWatchLogsOps,
		StepFunctionsOps:  ec.stepFunctionsOps,
		CloudWatchOps:     ec.cloudWatchOps,
		GlobalConfig:      ec.gCfg,
		Logger:            ctx.Logger,
	})

	return handler, nil
}
