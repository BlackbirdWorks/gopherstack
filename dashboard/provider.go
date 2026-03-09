package dashboard

import (
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	globalcfg "github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	"github.com/blackbirdworks/gopherstack/services/s3"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	sesv2backend "github.com/blackbirdworks/gopherstack/services/sesv2"
	"github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
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
	GetCloudFormationHandler() service.Registerable
	GetKinesisHandler() service.Registerable
	GetElastiCacheHandler() service.Registerable
	GetRoute53Handler() service.Registerable
	GetSESHandler() service.Registerable
	GetSESv2Handler() service.Registerable
	GetEC2Handler() service.Registerable
	GetOpenSearchHandler() service.Registerable
	GetACMHandler() service.Registerable
	GetRedshiftHandler() service.Registerable
	GetRDSHandler() service.Registerable
	GetAWSConfigHandler() service.Registerable
	GetS3ControlHandler() service.Registerable
	GetResourceGroupsHandler() service.Registerable
	GetSWFHandler() service.Registerable
	GetFirehoseHandler() service.Registerable
	GetCognitoIdentityHandler() service.Registerable
	GetAppSyncHandler() service.Registerable
	GetGlobalConfig() globalcfg.GlobalConfig
	GetFaultStore() *chaos.FaultStore
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
	stepFunctionsOps   *sfnbackend.Handler
	cloudWatchOps      *cwbackend.Handler
	ssmClient          *ssmsdk.Client
	ddb                *dynamodb.DynamoDBHandler
	s3h                *s3.S3Handler
	ssmOps             *ssm.Handler
	iamOps             *iambackend.Handler
	stsOps             *stsbackend.Handler
	snsOps             *sns.Handler
	sqsOps             *sqsbackend.Handler
	kmsOps             *kmsbackend.Handler
	secretsManagerOps  *secretsmanagerbackend.Handler
	lambdaOps          *lambdabackend.Handler
	eventBridgeOps     *ebbackend.Handler
	apiGatewayOps      *apigwbackend.Handler
	cloudWatchLogsOps  *cwlogsbackend.Handler
	s3Client           *s3sdk.Client
	ddbClient          *ddbsdk.Client
	cloudFormationOps  *cfnbackend.Handler
	kinesisOps         *kinesisbackend.Handler
	elasticacheOps     *elasticachebackend.Handler
	route53Ops         *route53backend.Handler
	sesOps             *sesbackend.Handler
	sesv2Ops           *sesv2backend.Handler
	ec2Ops             *ec2backend.Handler
	opensearchOps      *opensearchbackend.Handler
	acmOps             *acmbackend.Handler
	redshiftOps        *redshiftbackend.Handler
	rdsOps             *rdsbackend.Handler
	awsconfigOps       *awsconfigbackend.Handler
	s3controlOps       *s3controlbackend.Handler
	resourcegroupsOps  *resourcegroupsbackend.Handler
	swfOps             *swfbackend.Handler
	firehoseOps        *firehosebackend.Handler
	cognitoIdentityOps *cognitoidentitybackend.Handler
	appSyncOps         *appsyncbackend.Handler
	faultStore         *chaos.FaultStore
	gCfg               globalcfg.GlobalConfig
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
	ec.faultStore = ap.GetFaultStore()
	ec.ddb, _ = ap.GetDynamoDBHandler().(*dynamodb.DynamoDBHandler)
	ec.s3h, _ = ap.GetS3Handler().(*s3.S3Handler)
	ec.cloudFormationOps, _ = ap.GetCloudFormationHandler().(*cfnbackend.Handler)
	if h := ap.GetElastiCacheHandler(); h != nil {
		ec.elasticacheOps, _ = h.(*elasticachebackend.Handler)
	}

	extractIntegrationHandlers(ap, &ec)

	return ec
}

// extractIntegrationHandlers populates optional integration service handlers on ec.
func extractIntegrationHandlers(ap AWSSDKProvider, ec *extractedConfig) {
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

	extractMonitoringHandlers(ap, ec)
	extractLongTailHandlers(ap, ec)
}

// extractMonitoringHandlers populates integration/monitoring service handlers on ec.
//

func extractMonitoringHandlers(ap AWSSDKProvider, ec *extractedConfig) {
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

	if h := ap.GetKinesisHandler(); h != nil {
		ec.kinesisOps, _ = h.(*kinesisbackend.Handler)
	}

	if h := ap.GetRoute53Handler(); h != nil {
		ec.route53Ops, _ = h.(*route53backend.Handler)
	}

	if h := ap.GetSESHandler(); h != nil {
		ec.sesOps, _ = h.(*sesbackend.Handler)
	}

	if h := ap.GetEC2Handler(); h != nil {
		ec.ec2Ops, _ = h.(*ec2backend.Handler)
	}
}

// extractLongTailHandlers populates long-tail service handlers on ec.
//

func extractLongTailHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetOpenSearchHandler(); h != nil {
		ec.opensearchOps, _ = h.(*opensearchbackend.Handler)
	}

	if h := ap.GetACMHandler(); h != nil {
		ec.acmOps, _ = h.(*acmbackend.Handler)
	}

	if h := ap.GetRedshiftHandler(); h != nil {
		ec.redshiftOps, _ = h.(*redshiftbackend.Handler)
	}

	if h := ap.GetRDSHandler(); h != nil {
		ec.rdsOps, _ = h.(*rdsbackend.Handler)
	}

	if h := ap.GetAWSConfigHandler(); h != nil {
		ec.awsconfigOps, _ = h.(*awsconfigbackend.Handler)
	}

	if h := ap.GetS3ControlHandler(); h != nil {
		ec.s3controlOps, _ = h.(*s3controlbackend.Handler)
	}

	if h := ap.GetResourceGroupsHandler(); h != nil {
		ec.resourcegroupsOps, _ = h.(*resourcegroupsbackend.Handler)
	}

	if h := ap.GetSWFHandler(); h != nil {
		ec.swfOps, _ = h.(*swfbackend.Handler)
	}

	if h := ap.GetFirehoseHandler(); h != nil {
		ec.firehoseOps, _ = h.(*firehosebackend.Handler)
	}

	if h := ap.GetCognitoIdentityHandler(); h != nil {
		ec.cognitoIdentityOps, _ = h.(*cognitoidentitybackend.Handler)
	}

	if h := ap.GetAppSyncHandler(); h != nil {
		ec.appSyncOps, _ = h.(*appsyncbackend.Handler)
	}

	if h := ap.GetSESv2Handler(); h != nil {
		ec.sesv2Ops, _ = h.(*sesv2backend.Handler)
	}
}

//nolint:ireturn // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	ec := extractFromProvider(ctx)

	handler := NewHandler(Config{
		DDBClient:          ec.ddbClient,
		S3Client:           ec.s3Client,
		SSMClient:          ec.ssmClient,
		DDBOps:             ec.ddb,
		S3Ops:              ec.s3h,
		SSMOps:             ec.ssmOps,
		IAMOps:             ec.iamOps,
		STSOps:             ec.stsOps,
		SNSOps:             ec.snsOps,
		SQSOps:             ec.sqsOps,
		KMSOps:             ec.kmsOps,
		SecretsManagerOps:  ec.secretsManagerOps,
		LambdaOps:          ec.lambdaOps,
		EventBridgeOps:     ec.eventBridgeOps,
		APIGatewayOps:      ec.apiGatewayOps,
		CloudWatchLogsOps:  ec.cloudWatchLogsOps,
		StepFunctionsOps:   ec.stepFunctionsOps,
		CloudWatchOps:      ec.cloudWatchOps,
		CloudFormationOps:  ec.cloudFormationOps,
		KinesisOps:         ec.kinesisOps,
		ElastiCacheOps:     ec.elasticacheOps,
		Route53Ops:         ec.route53Ops,
		SESOps:             ec.sesOps,
		SESv2Ops:           ec.sesv2Ops,
		EC2Ops:             ec.ec2Ops,
		OpenSearchOps:      ec.opensearchOps,
		ACMOps:             ec.acmOps,
		RedshiftOps:        ec.redshiftOps,
		RDSOps:             ec.rdsOps,
		AWSConfigOps:       ec.awsconfigOps,
		S3ControlOps:       ec.s3controlOps,
		ResourceGroupsOps:  ec.resourcegroupsOps,
		SWFOps:             ec.swfOps,
		FirehoseOps:        ec.firehoseOps,
		CognitoIdentityOps: ec.cognitoIdentityOps,
		AppSyncOps:         ec.appSyncOps,
		GlobalConfig:       ec.gCfg,
		FaultStore:         ec.faultStore,
		Logger:             ctx.Logger,
	})

	return handler, nil
}
