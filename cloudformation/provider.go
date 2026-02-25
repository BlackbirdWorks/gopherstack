package cloudformation

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"
)

// BackendsProvider is a private interface to extract service backends for resource creation.
type BackendsProvider interface {
	GetDynamoDBHandler() service.Registerable
	GetS3Handler() service.Registerable
	GetSQSHandler() service.Registerable
	GetSNSHandler() service.Registerable
	GetSSMHandler() service.Registerable
	GetKMSHandler() service.Registerable
	GetSecretsManagerHandler() service.Registerable
	GetLambdaHandler() service.Registerable
	GetEventBridgeHandler() service.Registerable
	GetStepFunctionsHandler() service.Registerable
	GetCloudWatchLogsHandler() service.Registerable
	GetAPIGatewayHandler() service.Registerable
	GetGlobalConfig() config.GlobalConfig
}

// Provider implements service.Provider for the CloudFormation service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "CloudFormation" }

// extractBackends initializes service backends from the given BackendsProvider.
func extractBackends(bp BackendsProvider) *ServiceBackends {
	cfg := bp.GetGlobalConfig()
	backends := &ServiceBackends{
		AccountID: cfg.AccountID,
		Region:    cfg.Region,
	}

	if h := bp.GetDynamoDBHandler(); h != nil {
		backends.DynamoDB, _ = h.(*ddbbackend.DynamoDBHandler)
	}

	if h := bp.GetS3Handler(); h != nil {
		backends.S3, _ = h.(*s3backend.S3Handler)
	}

	if h := bp.GetSQSHandler(); h != nil {
		backends.SQS, _ = h.(*sqsbackend.Handler)
	}

	if h := bp.GetSNSHandler(); h != nil {
		backends.SNS, _ = h.(*snsbackend.Handler)
	}

	if h := bp.GetSSMHandler(); h != nil {
		backends.SSM, _ = h.(*ssmbackend.Handler)
	}

	if h := bp.GetKMSHandler(); h != nil {
		backends.KMS, _ = h.(*kmsbackend.Handler)
	}

	if h := bp.GetSecretsManagerHandler(); h != nil {
		backends.SecretsManager, _ = h.(*secretsmanagerbackend.Handler)
	}

	if h := bp.GetLambdaHandler(); h != nil {
		backends.Lambda, _ = h.(*lambdabackend.Handler)
	}

	if h := bp.GetEventBridgeHandler(); h != nil {
		backends.EventBridge, _ = h.(*ebbackend.Handler)
	}

	if h := bp.GetStepFunctionsHandler(); h != nil {
		backends.StepFunctions, _ = h.(*sfnbackend.Handler)
	}

	if h := bp.GetCloudWatchLogsHandler(); h != nil {
		backends.CloudWatchLogs, _ = h.(*cwlogsbackend.Handler)
	}

	if h := bp.GetAPIGatewayHandler(); h != nil {
		backends.APIGateway, _ = h.(*apigwbackend.Handler)
	}

	return backends
}

// Init initializes the CloudFormation service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := MockAccountID
	region := MockRegion

	var backends *ServiceBackends

	if bp, isBP := ctx.Config.(BackendsProvider); isBP {
		backends = extractBackends(bp)
		accountID = backends.AccountID
		region = backends.Region
	} else if cp, isCP := ctx.Config.(config.Provider); isCP {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	creator := NewResourceCreator(backends)
	backend := NewInMemoryBackendWithConfig(accountID, region, creator)
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
