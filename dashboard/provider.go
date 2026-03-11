package dashboard

import (
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigwmgmtbackend "github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	appconfigdatabackend "github.com/blackbirdworks/gopherstack/services/appconfigdata"
	applicationautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	bedrockbackend "github.com/blackbirdworks/gopherstack/services/bedrock"
	bedrockruntimebackend "github.com/blackbirdworks/gopherstack/services/bedrockruntime"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codeartifactbackend "github.com/blackbirdworks/gopherstack/services/codeartifact"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	codeconnectionsbackend "github.com/blackbirdworks/gopherstack/services/codeconnections"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	codestarconnectionsbackend "github.com/blackbirdworks/gopherstack/services/codestarconnections"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	dmsbackend "github.com/blackbirdworks/gopherstack/services/dms"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	iotdataplanebackend "github.com/blackbirdworks/gopherstack/services/iotdataplane"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	globalcfg "github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	elastictranscoderbackend "github.com/blackbirdworks/gopherstack/services/elastictranscoder"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	taggingbackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
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
	GetACMPCAHandler() service.Registerable
	GetRedshiftHandler() service.Registerable
	GetRDSHandler() service.Registerable
	GetAWSConfigHandler() service.Registerable
	GetS3ControlHandler() service.Registerable
	GetResourceGroupsHandler() service.Registerable
	GetResourceGroupsTaggingHandler() service.Registerable
	GetSWFHandler() service.Registerable
	GetFirehoseHandler() service.Registerable
	GetCognitoIdentityHandler() service.Registerable
	GetAppSyncHandler() service.Registerable
	GetCognitoIDPHandler() service.Registerable
	GetIoTDataPlaneHandler() service.Registerable
	GetAmplifyHandler() service.Registerable
	GetAutoscalingHandler() service.Registerable
	GetAPIGatewayV2Handler() service.Registerable
	GetAthenaHandler() service.Registerable
	GetBackupHandler() service.Registerable
	GetCloudTrailHandler() service.Registerable
	GetAppConfigHandler() service.Registerable
	GetApplicationAutoscalingHandler() service.Registerable
	GetBatchHandler() service.Registerable
	GetBedrockHandler() service.Registerable
	GetBedrockRuntimeHandler() service.Registerable
	GetCeHandler() service.Registerable
	GetCloudControlHandler() service.Registerable
	GetCloudFrontHandler() service.Registerable
	GetCodeArtifactHandler() service.Registerable
	GetCodeBuildHandler() service.Registerable
	GetCodeCommitHandler() service.Registerable
	GetCodePipelineHandler() service.Registerable
	GetCodeConnectionsHandler() service.Registerable
	GetCodeDeployHandler() service.Registerable
	GetDMSHandler() service.Registerable
	GetCodeStarConnectionsHandler() service.Registerable
	GetDynamoDBStreamsHandler() service.Registerable
	GetDocDBHandler() service.Registerable
	GetECRHandler() service.Registerable
	GetECSHandler() service.Registerable
	GetEFSHandler() service.Registerable
	GetEKSHandler() service.Registerable
	GetIoTHandler() service.Registerable
	GetFISHandler() service.Registerable
	GetAPIGatewayManagementAPIHandler() service.Registerable
	GetAppConfigDataHandler() service.Registerable
	GetElasticTranscoderHandler() service.Registerable
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
	stepFunctionsOps          *sfnbackend.Handler
	cloudWatchOps             *cwbackend.Handler
	ssmClient                 *ssmsdk.Client
	ddb                       *dynamodb.DynamoDBHandler
	s3h                       *s3.S3Handler
	ssmOps                    *ssm.Handler
	iamOps                    *iambackend.Handler
	stsOps                    *stsbackend.Handler
	snsOps                    *sns.Handler
	sqsOps                    *sqsbackend.Handler
	kmsOps                    *kmsbackend.Handler
	secretsManagerOps         *secretsmanagerbackend.Handler
	lambdaOps                 *lambdabackend.Handler
	eventBridgeOps            *ebbackend.Handler
	apiGatewayOps             *apigwbackend.Handler
	cloudWatchLogsOps         *cwlogsbackend.Handler
	s3Client                  *s3sdk.Client
	ddbClient                 *ddbsdk.Client
	cloudFormationOps         *cfnbackend.Handler
	kinesisOps                *kinesisbackend.Handler
	elasticacheOps            *elasticachebackend.Handler
	route53Ops                *route53backend.Handler
	sesOps                    *sesbackend.Handler
	sesv2Ops                  *sesv2backend.Handler
	ec2Ops                    *ec2backend.Handler
	opensearchOps             *opensearchbackend.Handler
	acmOps                    *acmbackend.Handler
	acmpcaOps                 *acmpcabackend.Handler
	redshiftOps               *redshiftbackend.Handler
	rdsOps                    *rdsbackend.Handler
	awsconfigOps              *awsconfigbackend.Handler
	s3controlOps              *s3controlbackend.Handler
	resourcegroupsOps         *resourcegroupsbackend.Handler
	resourcegroupstaggingOps  *taggingbackend.Handler
	swfOps                    *swfbackend.Handler
	firehoseOps               *firehosebackend.Handler
	cognitoIdentityOps        *cognitoidentitybackend.Handler
	appSyncOps                *appsyncbackend.Handler
	cognitoIDPOps             *cognitoidpbackend.Handler
	iotDataPlaneOps           *iotdataplanebackend.Handler
	apiGatewayMgmtOps         *apigwmgmtbackend.Handler
	apiGatewayV2Ops           *apigwv2backend.Handler
	appConfigDataOps          *appconfigdatabackend.Handler
	amplifyOps                *amplifybackend.Handler
	athenaOps                 *athenabackend.Handler
	autoscalingOps            *autoscalingbackend.Handler
	backupOps                 *backupbackend.Handler
	cloudtrailOps             *cloudtrailbackend.Handler
	appConfigOps              *appconfigbackend.Handler
	applicationAutoscalingOps *applicationautoscalingbackend.Handler
	batchOps                  *batchbackend.Handler
	bedrockOps                *bedrockbackend.Handler
	bedrockRuntimeOps         *bedrockruntimebackend.Handler
	ceOps                     *cebackend.Handler
	cloudcontrolOps           *cloudcontrolbackend.Handler
	cloudFrontOps             *cloudfrontbackend.Handler
	codeArtifactOps           *codeartifactbackend.Handler
	codebuildOps              *codebuildbackend.Handler
	codeCommitOps             *codecommitbackend.Handler
	codePipelineOps           *codepipelinebackend.Handler
	codeConnectionsOps        *codeconnectionsbackend.Handler
	codeDeployOps             *codedeploybackend.Handler
	dmsOps                    *dmsbackend.Handler
	codeStarConnectionsOps    *codestarconnectionsbackend.Handler
	dynamodbStreamsOps        *dynamodbstreams.Handler
	docdbOps                  *docdbbackend.Handler
	ecrOps                    *ecrbackend.Handler
	ecsOps                    *ecsbackend.Handler
	efsOps                    *efsbackend.Handler
	eksOps                    *eksbackend.Handler
	iotOps                    *iotbackend.Handler
	fisOps                    *fisbackend.Handler
	elasticTranscoderOps      *elastictranscoderbackend.Handler
	faultStore                *chaos.FaultStore
	gCfg                      globalcfg.GlobalConfig
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

	if h := ap.GetACMPCAHandler(); h != nil {
		ec.acmpcaOps, _ = h.(*acmpcabackend.Handler)
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

	if h := ap.GetResourceGroupsTaggingHandler(); h != nil {
		ec.resourcegroupstaggingOps, _ = h.(*taggingbackend.Handler)
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

	extractRecentHandlers(ap, ec)
}

func extractRecentHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetAppSyncHandler(); h != nil {
		ec.appSyncOps, _ = h.(*appsyncbackend.Handler)
	}

	if h := ap.GetCognitoIDPHandler(); h != nil {
		ec.cognitoIDPOps, _ = h.(*cognitoidpbackend.Handler)
	}

	if h := ap.GetIoTDataPlaneHandler(); h != nil {
		ec.iotDataPlaneOps, _ = h.(*iotdataplanebackend.Handler)
	}

	if h := ap.GetAmplifyHandler(); h != nil {
		ec.amplifyOps, _ = h.(*amplifybackend.Handler)
	}

	if h := ap.GetAPIGatewayV2Handler(); h != nil {
		ec.apiGatewayV2Ops, _ = h.(*apigwv2backend.Handler)
	}

	if h := ap.GetSESv2Handler(); h != nil {
		ec.sesv2Ops, _ = h.(*sesv2backend.Handler)
	}

	if h := ap.GetBedrockRuntimeHandler(); h != nil {
		ec.bedrockRuntimeOps, _ = h.(*bedrockruntimebackend.Handler)
	}

	extractECRECSAndIoTHandlers(ap, ec)
}

// extractECRECSAndIoTHandlers populates ECR, ECS, and IoT handlers on ec.
func extractECRECSAndIoTHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetECRHandler(); h != nil {
		ec.ecrOps, _ = h.(*ecrbackend.Handler)
	}

	if h := ap.GetEFSHandler(); h != nil {
		ec.efsOps, _ = h.(*efsbackend.Handler)
	}

	extractContainerAndFaultHandlers(ap, ec)
}

// extractContainerAndFaultHandlers populates container and fault injection service handlers on ec.
func extractContainerAndFaultHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetECSHandler(); h != nil {
		ec.ecsOps, _ = h.(*ecsbackend.Handler)
	}

	if h := ap.GetEKSHandler(); h != nil {
		ec.eksOps, _ = h.(*eksbackend.Handler)
	}

	if h := ap.GetIoTHandler(); h != nil {
		ec.iotOps, _ = h.(*iotbackend.Handler)
	}

	if h := ap.GetFISHandler(); h != nil {
		ec.fisOps, _ = h.(*fisbackend.Handler)
	}

	if h := ap.GetAPIGatewayManagementAPIHandler(); h != nil {
		ec.apiGatewayMgmtOps, _ = h.(*apigwmgmtbackend.Handler)
	}

	if h := ap.GetAppConfigHandler(); h != nil {
		ec.appConfigOps, _ = h.(*appconfigbackend.Handler)
	}

	if h := ap.GetAppConfigDataHandler(); h != nil {
		ec.appConfigDataOps, _ = h.(*appconfigdatabackend.Handler)
	}

	if h := ap.GetApplicationAutoscalingHandler(); h != nil {
		ec.applicationAutoscalingOps, _ = h.(*applicationautoscalingbackend.Handler)
	}

	if h := ap.GetAthenaHandler(); h != nil {
		ec.athenaOps, _ = h.(*athenabackend.Handler)
	}

	extractLatestServiceHandlers(ap, ec)
}

// extractLatestServiceHandlers populates handlers for the most recently added services.
func extractLatestServiceHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetBackupHandler(); h != nil {
		ec.backupOps, _ = h.(*backupbackend.Handler)
	}

	if h := ap.GetCloudTrailHandler(); h != nil {
		ec.cloudtrailOps, _ = h.(*cloudtrailbackend.Handler)
	}

	if h := ap.GetAutoscalingHandler(); h != nil {
		ec.autoscalingOps, _ = h.(*autoscalingbackend.Handler)
	}

	if h := ap.GetBatchHandler(); h != nil {
		ec.batchOps, _ = h.(*batchbackend.Handler)
	}

	if h := ap.GetBedrockHandler(); h != nil {
		ec.bedrockOps, _ = h.(*bedrockbackend.Handler)
	}

	extractCloudPlatformHandlers(ap, ec)
}

// extractCloudPlatformHandlers populates CE, CloudControl, and CloudFront handlers on ec.
func extractCloudPlatformHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetCeHandler(); h != nil {
		ec.ceOps, _ = h.(*cebackend.Handler)
	}

	if h := ap.GetCloudControlHandler(); h != nil {
		ec.cloudcontrolOps, _ = h.(*cloudcontrolbackend.Handler)
	}

	if h := ap.GetCloudFrontHandler(); h != nil {
		ec.cloudFrontOps, _ = h.(*cloudfrontbackend.Handler)
	}

	extractCodeServiceHandlers(ap, ec)
}

// extractCodeServiceHandlers populates CodeArtifact, CodeBuild, CodeCommit, CodePipeline,
// CodeConnections, CodeDeploy, and ElasticTranscoder handlers on ec.
func extractCodeServiceHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetCodeArtifactHandler(); h != nil {
		ec.codeArtifactOps, _ = h.(*codeartifactbackend.Handler)
	}

	if h := ap.GetCodeBuildHandler(); h != nil {
		ec.codebuildOps, _ = h.(*codebuildbackend.Handler)
	}

	if h := ap.GetElasticTranscoderHandler(); h != nil {
		ec.elasticTranscoderOps, _ = h.(*elastictranscoderbackend.Handler)
	}

	extractCodeHandlers(ap, ec)
}

// extractCodeHandlers populates Code* service handlers on ec.
func extractCodeHandlers(ap AWSSDKProvider, ec *extractedConfig) {
	if h := ap.GetCodeCommitHandler(); h != nil {
		ec.codeCommitOps, _ = h.(*codecommitbackend.Handler)
	}

	if h := ap.GetCodePipelineHandler(); h != nil {
		ec.codePipelineOps, _ = h.(*codepipelinebackend.Handler)
	}

	if h := ap.GetCodeConnectionsHandler(); h != nil {
		ec.codeConnectionsOps, _ = h.(*codeconnectionsbackend.Handler)
	}

	if h := ap.GetCodeDeployHandler(); h != nil {
		ec.codeDeployOps, _ = h.(*codedeploybackend.Handler)
	}

	if h := ap.GetDMSHandler(); h != nil {
		ec.dmsOps, _ = h.(*dmsbackend.Handler)
	}

	if h := ap.GetCodeStarConnectionsHandler(); h != nil {
		ec.codeStarConnectionsOps, _ = h.(*codestarconnectionsbackend.Handler)
	}

	if h := ap.GetDynamoDBStreamsHandler(); h != nil {
		ec.dynamodbStreamsOps, _ = h.(*dynamodbstreams.Handler)
	}

	if h := ap.GetDocDBHandler(); h != nil {
		ec.docdbOps, _ = h.(*docdbbackend.Handler)
	}
}

//nolint:ireturn // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	ec := extractFromProvider(ctx)

	handler := NewHandler(Config{
		DDBClient:                  ec.ddbClient,
		S3Client:                   ec.s3Client,
		SSMClient:                  ec.ssmClient,
		DDBOps:                     ec.ddb,
		S3Ops:                      ec.s3h,
		SSMOps:                     ec.ssmOps,
		IAMOps:                     ec.iamOps,
		STSOps:                     ec.stsOps,
		SNSOps:                     ec.snsOps,
		SQSOps:                     ec.sqsOps,
		KMSOps:                     ec.kmsOps,
		SecretsManagerOps:          ec.secretsManagerOps,
		LambdaOps:                  ec.lambdaOps,
		EventBridgeOps:             ec.eventBridgeOps,
		APIGatewayOps:              ec.apiGatewayOps,
		CloudWatchLogsOps:          ec.cloudWatchLogsOps,
		StepFunctionsOps:           ec.stepFunctionsOps,
		CloudWatchOps:              ec.cloudWatchOps,
		CloudFormationOps:          ec.cloudFormationOps,
		KinesisOps:                 ec.kinesisOps,
		ElastiCacheOps:             ec.elasticacheOps,
		Route53Ops:                 ec.route53Ops,
		SESOps:                     ec.sesOps,
		SESv2Ops:                   ec.sesv2Ops,
		EC2Ops:                     ec.ec2Ops,
		OpenSearchOps:              ec.opensearchOps,
		ACMOps:                     ec.acmOps,
		ACMPCAOps:                  ec.acmpcaOps,
		RedshiftOps:                ec.redshiftOps,
		RDSOps:                     ec.rdsOps,
		AWSConfigOps:               ec.awsconfigOps,
		S3ControlOps:               ec.s3controlOps,
		ResourceGroupsOps:          ec.resourcegroupsOps,
		ResourceGroupsTaggingOps:   ec.resourcegroupstaggingOps,
		SWFOps:                     ec.swfOps,
		FirehoseOps:                ec.firehoseOps,
		CognitoIdentityOps:         ec.cognitoIdentityOps,
		AppSyncOps:                 ec.appSyncOps,
		CognitoIDPOps:              ec.cognitoIDPOps,
		IoTDataPlaneOps:            ec.iotDataPlaneOps,
		APIGatewayManagementAPIOps: ec.apiGatewayMgmtOps,
		APIGatewayV2Ops:            ec.apiGatewayV2Ops,
		AppConfigDataOps:           ec.appConfigDataOps,
		AmplifyOps:                 ec.amplifyOps,
		AthenaOps:                  ec.athenaOps,
		AutoscalingOps:             ec.autoscalingOps,
		BackupOps:                  ec.backupOps,
		CloudTrailOps:              ec.cloudtrailOps,
		AppConfigOps:               ec.appConfigOps,
		ApplicationAutoscalingOps:  ec.applicationAutoscalingOps,
		BatchOps:                   ec.batchOps,
		BedrockOps:                 ec.bedrockOps,
		BedrockRuntimeOps:          ec.bedrockRuntimeOps,
		CeOps:                      ec.ceOps,
		CloudControlOps:            ec.cloudcontrolOps,
		CloudFrontOps:              ec.cloudFrontOps,
		CodeArtifactOps:            ec.codeArtifactOps,
		CodeBuildOps:               ec.codebuildOps,
		CodeCommitOps:              ec.codeCommitOps,
		CodePipelineOps:            ec.codePipelineOps,
		CodeConnectionsOps:         ec.codeConnectionsOps,
		CodeDeployOps:              ec.codeDeployOps,
		DMSOps:                     ec.dmsOps,
		CodeStarConnectionsOps:     ec.codeStarConnectionsOps,
		DynamoDBStreamsOps:         ec.dynamodbStreamsOps,
		DocDBOps:                   ec.docdbOps,
		ECROps:                     ec.ecrOps,
		ECSOps:                     ec.ecsOps,
		EFSOps:                     ec.efsOps,
		EKSOps:                     ec.eksOps,
		IoTOps:                     ec.iotOps,
		FISOps:                     ec.fisOps,
		ElasticTranscoderOps:       ec.elasticTranscoderOps,
		GlobalConfig:               ec.gCfg,
		FaultStore:                 ec.faultStore,
		Logger:                     ctx.Logger,
	})

	return handler, nil
}
