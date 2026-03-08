package cloudformation

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
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
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
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
	GetIAMHandler() service.Registerable
	GetEC2Handler() service.Registerable
	GetKinesisHandler() service.Registerable
	GetCloudWatchHandler() service.Registerable
	GetRoute53Handler() service.Registerable
	GetElastiCacheHandler() service.Registerable
	GetSchedulerHandler() service.Registerable
	GetRDSHandler() service.Registerable
	GetECSHandler() service.Registerable
	GetECRHandler() service.Registerable
	GetRedshiftHandler() service.Registerable
	GetOpenSearchHandler() service.Registerable
	GetFirehoseHandler() service.Registerable
	GetRoute53ResolverHandler() service.Registerable
	GetSWFHandler() service.Registerable
	GetAppSyncHandler() service.Registerable
	GetSESHandler() service.Registerable
	GetACMHandler() service.Registerable
	GetCognitoIDPHandler() service.Registerable
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

	extractCoreBackends(bp, backends)
	extractExtendedBackends(bp, backends)
	extractServiceBackends(bp, backends)

	return backends
}

// extractCoreBackends populates the core service backends (DynamoDB, S3, SQS, etc.).
func extractCoreBackends(bp BackendsProvider, backends *ServiceBackends) {
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
}

// extractExtendedBackends populates the extended service backends (Lambda, IAM, EC2, etc.).
//
//nolint:dupl // architecturally identical to extractServiceBackends but operates on different service types
func extractExtendedBackends(bp BackendsProvider, backends *ServiceBackends) {
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

	if h := bp.GetIAMHandler(); h != nil {
		backends.IAM, _ = h.(*iambackend.Handler)
	}

	if h := bp.GetEC2Handler(); h != nil {
		backends.EC2, _ = h.(*ec2backend.Handler)
	}

	if h := bp.GetKinesisHandler(); h != nil {
		backends.Kinesis, _ = h.(*kinesisbackend.Handler)
	}

	if h := bp.GetCloudWatchHandler(); h != nil {
		backends.CloudWatch, _ = h.(*cloudwatchbackend.Handler)
	}

	if h := bp.GetRoute53Handler(); h != nil {
		backends.Route53, _ = h.(*route53backend.Handler)
	}

	if h := bp.GetElastiCacheHandler(); h != nil {
		backends.ElastiCache, _ = h.(*elasticachebackend.Handler)
	}

	if h := bp.GetSchedulerHandler(); h != nil {
		backends.Scheduler, _ = h.(*schedulerbackend.Handler)
	}
}

// extractServiceBackends populates the newer service backends (RDS, ECS, ECR, etc.).
//
//nolint:dupl // architecturally identical to extractExtendedBackends but operates on different service types
func extractServiceBackends(bp BackendsProvider, backends *ServiceBackends) {
	if h := bp.GetRDSHandler(); h != nil {
		backends.RDS, _ = h.(*rdsbackend.Handler)
	}

	if h := bp.GetECSHandler(); h != nil {
		backends.ECS, _ = h.(*ecsbackend.Handler)
	}

	if h := bp.GetECRHandler(); h != nil {
		backends.ECR, _ = h.(*ecrbackend.Handler)
	}

	if h := bp.GetRedshiftHandler(); h != nil {
		backends.Redshift, _ = h.(*redshiftbackend.Handler)
	}

	if h := bp.GetOpenSearchHandler(); h != nil {
		backends.OpenSearch, _ = h.(*opensearchbackend.Handler)
	}

	if h := bp.GetFirehoseHandler(); h != nil {
		backends.Firehose, _ = h.(*firehosebackend.Handler)
	}

	if h := bp.GetRoute53ResolverHandler(); h != nil {
		backends.Route53Resolver, _ = h.(*route53resolverbackend.Handler)
	}

	if h := bp.GetSWFHandler(); h != nil {
		backends.SWF, _ = h.(*swfbackend.Handler)
	}

	if h := bp.GetAppSyncHandler(); h != nil {
		backends.AppSync, _ = h.(*appsyncbackend.Handler)
	}

	if h := bp.GetSESHandler(); h != nil {
		backends.SES, _ = h.(*sesbackend.Handler)
	}

	if h := bp.GetACMHandler(); h != nil {
		backends.ACM, _ = h.(*acmbackend.Handler)
	}

	if h := bp.GetCognitoIDPHandler(); h != nil {
		backends.CognitoIDP, _ = h.(*cognitoidpbackend.Handler)
	}
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
	handler := NewHandler(backend)

	return handler, nil
}
