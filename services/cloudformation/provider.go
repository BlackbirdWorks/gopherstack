package cloudformation

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
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
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
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
	// Phase-3 handlers
	GetEKSHandler() service.Registerable
	GetEFSHandler() service.Registerable
	GetBatchHandler() service.Registerable
	GetCloudFrontHandler() service.Registerable
	GetAutoscalingHandler() service.Registerable
	GetAPIGatewayV2Handler() service.Registerable
	GetCodeBuildHandler() service.Registerable
	GetGlueHandler() service.Registerable
	GetDocDBHandler() service.Registerable
	GetNeptuneHandler() service.Registerable
	GetKafkaHandler() service.Registerable
	GetTransferHandler() service.Registerable
	GetCloudTrailHandler() service.Registerable
	GetCodePipelineHandler() service.Registerable
	GetIoTHandler() service.Registerable
	GetPipesHandler() service.Registerable
	GetEMRHandler() service.Registerable
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
	extractAllServiceBackends(bp, backends)

	return backends
}

// extractCoreBackends populates the core service backends (DynamoDB, S3, SQS, etc.).
func extractCoreBackends(bp BackendsProvider, backends *ServiceBackends) {
	backends.DynamoDB, _ = getHandler[*ddbbackend.DynamoDBHandler](bp.GetDynamoDBHandler())
	backends.S3, _ = getHandler[*s3backend.S3Handler](bp.GetS3Handler())
	backends.SQS, _ = getHandler[*sqsbackend.Handler](bp.GetSQSHandler())
	backends.SNS, _ = getHandler[*snsbackend.Handler](bp.GetSNSHandler())
	backends.SSM, _ = getHandler[*ssmbackend.Handler](bp.GetSSMHandler())
	backends.KMS, _ = getHandler[*kmsbackend.Handler](bp.GetKMSHandler())
	backends.SecretsManager, _ = getHandler[*secretsmanagerbackend.Handler](bp.GetSecretsManagerHandler())
}

// extractAllServiceBackends populates all extended and phase-2 service backends.
func extractAllServiceBackends(bp BackendsProvider, backends *ServiceBackends) {
	// Extended backends (Lambda, IAM, EC2, etc.)
	backends.Lambda, _ = getHandler[*lambdabackend.Handler](bp.GetLambdaHandler())
	backends.EventBridge, _ = getHandler[*ebbackend.Handler](bp.GetEventBridgeHandler())
	backends.StepFunctions, _ = getHandler[*sfnbackend.Handler](bp.GetStepFunctionsHandler())
	backends.CloudWatchLogs, _ = getHandler[*cwlogsbackend.Handler](bp.GetCloudWatchLogsHandler())
	backends.APIGateway, _ = getHandler[*apigwbackend.Handler](bp.GetAPIGatewayHandler())
	backends.IAM, _ = getHandler[*iambackend.Handler](bp.GetIAMHandler())
	backends.EC2, _ = getHandler[*ec2backend.Handler](bp.GetEC2Handler())
	backends.Kinesis, _ = getHandler[*kinesisbackend.Handler](bp.GetKinesisHandler())
	backends.CloudWatch, _ = getHandler[*cloudwatchbackend.Handler](bp.GetCloudWatchHandler())
	backends.Route53, _ = getHandler[*route53backend.Handler](bp.GetRoute53Handler())
	backends.ElastiCache, _ = getHandler[*elasticachebackend.Handler](bp.GetElastiCacheHandler())
	backends.Scheduler, _ = getHandler[*schedulerbackend.Handler](bp.GetSchedulerHandler())
	// Phase-2 backends (RDS, ECS, ECR, etc.)
	backends.RDS, _ = getHandler[*rdsbackend.Handler](bp.GetRDSHandler())
	backends.ECS, _ = getHandler[*ecsbackend.Handler](bp.GetECSHandler())
	backends.ECR, _ = getHandler[*ecrbackend.Handler](bp.GetECRHandler())
	backends.Redshift, _ = getHandler[*redshiftbackend.Handler](bp.GetRedshiftHandler())
	backends.OpenSearch, _ = getHandler[*opensearchbackend.Handler](bp.GetOpenSearchHandler())
	backends.Firehose, _ = getHandler[*firehosebackend.Handler](bp.GetFirehoseHandler())
	backends.Route53Resolver, _ = getHandler[*route53resolverbackend.Handler](bp.GetRoute53ResolverHandler())
	backends.SWF, _ = getHandler[*swfbackend.Handler](bp.GetSWFHandler())
	backends.AppSync, _ = getHandler[*appsyncbackend.Handler](bp.GetAppSyncHandler())
	backends.SES, _ = getHandler[*sesbackend.Handler](bp.GetSESHandler())
	backends.ACM, _ = getHandler[*acmbackend.Handler](bp.GetACMHandler())
	backends.CognitoIDP, _ = getHandler[*cognitoidpbackend.Handler](bp.GetCognitoIDPHandler())
	// Phase-3 backends (EKS, EFS, Batch, CloudFront, AutoScaling, etc.)
	backends.EKS, _ = getHandler[*eksbackend.Handler](bp.GetEKSHandler())
	backends.EFS, _ = getHandler[*efsbackend.Handler](bp.GetEFSHandler())
	backends.Batch, _ = getHandler[*batchbackend.Handler](bp.GetBatchHandler())
	backends.CloudFront, _ = getHandler[*cloudfrontbackend.Handler](bp.GetCloudFrontHandler())
	backends.Autoscaling, _ = getHandler[*autoscalingbackend.Handler](bp.GetAutoscalingHandler())
	backends.APIGatewayV2, _ = getHandler[*apigatewayv2backend.Handler](bp.GetAPIGatewayV2Handler())
	backends.CodeBuild, _ = getHandler[*codebuildbackend.Handler](bp.GetCodeBuildHandler())
	backends.Glue, _ = getHandler[*gluebackend.Handler](bp.GetGlueHandler())
	backends.DocDB, _ = getHandler[*docdbbackend.Handler](bp.GetDocDBHandler())
	backends.Neptune, _ = getHandler[*neptunebackend.Handler](bp.GetNeptuneHandler())
	backends.Kafka, _ = getHandler[*kafkabackend.Handler](bp.GetKafkaHandler())
	backends.Transfer, _ = getHandler[*transferbackend.Handler](bp.GetTransferHandler())
	backends.CloudTrail, _ = getHandler[*cloudtrailbackend.Handler](bp.GetCloudTrailHandler())
	backends.CodePipeline, _ = getHandler[*codepipelinebackend.Handler](bp.GetCodePipelineHandler())
	backends.IoT, _ = getHandler[*iotbackend.Handler](bp.GetIoTHandler())
	backends.Pipes, _ = getHandler[*pipesbackend.Handler](bp.GetPipesHandler())
	backends.EMR, _ = getHandler[*emrbackend.Handler](bp.GetEMRHandler())
}

// getHandler asserts h to type T; returns zero value and false if h is nil or the wrong type.
func getHandler[T any](h service.Registerable) (T, bool) {
	if h == nil {
		var zero T

		return zero, false
	}

	v, ok := h.(T)

	return v, ok
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
