package teststack

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	dynamodbstreamsbackend "github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	elasticbeanstalkbackend "github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
	elastictranscoderbackend "github.com/blackbirdworks/gopherstack/services/elastictranscoder"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	emrserverlessbackend "github.com/blackbirdworks/gopherstack/services/emrserverless"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	glacierbackend "github.com/blackbirdworks/gopherstack/services/glacier"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	iotdataplanebackend "github.com/blackbirdworks/gopherstack/services/iotdataplane"
	iotwirelessbackend "github.com/blackbirdworks/gopherstack/services/iotwireless"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kinesisanalyticsbackend "github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
	kinesisanalyticsv2backend "github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lakeformationbackend "github.com/blackbirdworks/gopherstack/services/lakeformation"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	mediaconvertbackend "github.com/blackbirdworks/gopherstack/services/mediaconvert"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	mediastoredatabackend "github.com/blackbirdworks/gopherstack/services/mediastoredata"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	mwaabackend "github.com/blackbirdworks/gopherstack/services/mwaa"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	qldbbackend "github.com/blackbirdworks/gopherstack/services/qldb"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	rgtabackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	sesv2backend "github.com/blackbirdworks/gopherstack/services/sesv2"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
)

const (
	// DynamoDB provisioned throughput for test tables.
	testReadCapacityUnits  = 5
	testWriteCapacityUnits = 5
)

// Stack holds a fully wired in-memory test stack with all services,
// the Echo router (correctly mounted), AWS SDK clients, and the dashboard handler.
type Stack struct {
	Echo                           *echo.Echo
	S3Backend                      *s3backend.InMemoryBackend
	S3Handler                      *s3backend.S3Handler
	DDBHandler                     *ddbbackend.DynamoDBHandler
	IAMBackend                     *iambackend.InMemoryBackend
	IAMHandler                     *iambackend.Handler
	STSHandler                     *stsbackend.Handler
	SNSHandler                     *snsbackend.Handler
	SQSHandler                     *sqsbackend.Handler
	KMSHandler                     *kmsbackend.Handler
	SecretsManagerHandler          *smbackend.Handler
	LambdaHandler                  *lambdabackend.Handler
	EventBridgeHandler             *ebbackend.Handler
	APIGatewayHandler              *apigwbackend.Handler
	CloudWatchLogsHandler          *cwlogsbackend.Handler
	StepFunctionsHandler           *sfnbackend.Handler
	CloudWatchHandler              *cwbackend.Handler
	CloudFormationHandler          *cfnbackend.Handler
	KinesisHandler                 *kinesisbackend.Handler
	ElastiCacheHandler             *elasticachebackend.Handler
	Route53Handler                 *route53backend.Handler
	SESHandler                     *sesbackend.Handler
	SESv2Handler                   *sesv2backend.Handler
	EC2Handler                     *ec2backend.Handler
	ECRHandler                     *ecrbackend.Handler
	ECSHandler                     *ecsbackend.Handler
	IoTHandler                     *iotbackend.Handler
	FISHandler                     *fisbackend.Handler
	IdentityStoreHandler           *identitystorebackend.Handler
	OpenSearchHandler              *opensearchbackend.Handler
	ACMHandler                     *acmbackend.Handler
	ACMPCAHandler                  *acmpcabackend.Handler
	RedshiftHandler                *redshiftbackend.Handler
	RDSHandler                     *rdsbackend.Handler
	DocDBHandler                   *docdbbackend.Handler
	NeptuneHandler                 *neptunebackend.Handler
	AWSConfigHandler               *awsconfigbackend.Handler
	S3ControlHandler               *s3controlbackend.Handler
	ResourceGroupsHandler          *resourcegroupsbackend.Handler
	ResourceGroupsTaggingHandler   *rgtabackend.Handler
	SWFHandler                     *swfbackend.Handler
	FirehoseHandler                *firehosebackend.Handler
	SchedulerHandler               *schedulerbackend.Handler
	Route53ResolverHandler         *route53resolverbackend.Handler
	TranscribeHandler              *transcribebackend.Handler
	SupportHandler                 *supportbackend.Handler
	CognitoIdentityHandler         *cognitoidentitybackend.Handler
	AppSyncHandler                 *appsyncbackend.Handler
	CognitoIDPHandler              *cognitoidpbackend.Handler
	IoTDataPlaneHandler            *iotdataplanebackend.Handler
	APIGatewayManagementAPIHandler *apigwmgmtbackend.Handler
	AppConfigDataHandler           *appconfigdatabackend.Handler
	AmplifyHandler                 *amplifybackend.Handler
	APIGatewayV2Handler            *apigwv2backend.Handler
	AppConfigHandler               *appconfigbackend.Handler
	AthenaHandler                  *athenabackend.Handler
	AutoscalingHandler             *autoscalingbackend.Handler
	ApplicationAutoscalingHandler  *applicationautoscalingbackend.Handler
	BackupHandler                  *backupbackend.Handler
	CloudTrailHandler              *cloudtrailbackend.Handler
	BatchHandler                   *batchbackend.Handler
	BedrockHandler                 *bedrockbackend.Handler
	BedrockRuntimeHandler          *bedrockruntimebackend.Handler
	CeHandler                      *cebackend.Handler
	CloudControlHandler            *cloudcontrolbackend.Handler
	CloudFrontHandler              *cloudfrontbackend.Handler
	// CodeArtifactHandler provides access to the CodeArtifact backend.
	CodeArtifactHandler *codeartifactbackend.Handler
	// CodeBuildHandler provides access to the CodeBuild backend.
	CodeBuildHandler *codebuildbackend.Handler
	// CodeCommitHandler provides access to the CodeCommit backend.
	CodeCommitHandler *codecommitbackend.Handler
	// CodePipelineHandler provides access to the CodePipeline backend.
	CodePipelineHandler *codepipelinebackend.Handler
	// CodeConnectionsHandler provides access to the CodeConnections backend.
	CodeConnectionsHandler *codeconnectionsbackend.Handler
	// CodeDeployHandler provides access to the CodeDeploy backend.
	CodeDeployHandler *codedeploybackend.Handler
	// DMSHandler provides access to the DMS backend.
	DMSHandler *dmsbackend.Handler
	// CodeStarConnectionsHandler provides access to the CodeStar Connections backend.
	CodeStarConnectionsHandler *codestarconnectionsbackend.Handler
	// DynamoDBStreamsHandler provides access to the DynamoDB Streams backend.
	DynamoDBStreamsHandler *dynamodbstreamsbackend.Handler
	// ElasticbeanstalkHandler provides access to the Elastic Beanstalk backend.
	ElasticbeanstalkHandler *elasticbeanstalkbackend.Handler
	// EFSHandler provides access to the EFS backend.
	EFSHandler *efsbackend.Handler
	// EKSHandler provides access to the EKS backend.
	EKSHandler *eksbackend.Handler
	// ElasticTranscoderHandler provides access to the Elastic Transcoder backend.
	ElasticTranscoderHandler *elastictranscoderbackend.Handler
	// ELBHandler provides access to the Classic ELB backend.
	ELBHandler *elbbackend.Handler
	// ELBv2Handler provides access to the ELBv2 (ALB/NLB) backend.
	ELBv2Handler *elbv2backend.Handler
	// EmrServerlessHandler provides access to the EMR Serverless backend.
	EmrServerlessHandler *emrserverlessbackend.Handler
	// EMRHandler provides access to the EMR backend.
	EMRHandler *emrbackend.Handler
	// GlacierHandler provides access to the Glacier backend.
	GlacierHandler *glacierbackend.Handler
	// IoTAnalyticsHandler provides access to the IoT Analytics backend.
	IoTAnalyticsHandler *iotanalyticsbackend.Handler
	// IoTWirelessHandler provides access to the IoT Wireless backend.
	IoTWirelessHandler *iotwirelessbackend.Handler
	// KinesisAnalyticsHandler provides access to the Kinesis Analytics backend.
	KinesisAnalyticsHandler *kinesisanalyticsbackend.Handler
	// KafkaHandler provides access to the MSK Kafka backend.
	KafkaHandler *kafkabackend.Handler
	// KinesisAnalyticsV2Handler provides access to the Kinesis Data Analytics v2 backend.
	KinesisAnalyticsV2Handler *kinesisanalyticsv2backend.Handler
	// LakeFormationHandler provides access to the Lake Formation backend.
	LakeFormationHandler *lakeformationbackend.Handler
	// MediaConvertHandler provides access to the MediaConvert backend.
	MediaConvertHandler *mediaconvertbackend.Handler
	// MQHandler provides access to the Amazon MQ backend.
	MQHandler *mqbackend.Handler
	// MediaStoreHandler provides access to the MediaStore backend.
	MediaStoreHandler *mediastorebackend.Handler
	// MediaStoreDataHandler provides access to the MediaStore Data backend.
	MediaStoreDataHandler *mediastoredatabackend.Handler
	// MemoryDBHandler provides access to the MemoryDB backend.
	MemoryDBHandler *memorydbbackend.Handler
	// MWAAHandler provides access to the MWAA backend.
	MWAAHandler *mwaabackend.Handler
	// PipesHandler provides access to the EventBridge Pipes backend.
	PipesHandler *pipesbackend.Handler
	// QLDBHandler provides access to the QLDB backend.
	QLDBHandler *qldbbackend.Handler
	S3Client    *s3.Client
	DDBClient   *dynamodb.Client
	FaultStore  *chaos.FaultStore
	Dashboard   *dashboard.DashboardHandler
}

// sdkClients holds the AWS SDK clients wired through the in-memory test server.
type sdkClients struct {
	DDB *dynamodb.Client
	S3  *s3.Client
	SSM *ssmsdk.Client
}

// newSDKClients creates AWS SDK clients pointed at the in-memory Echo server.
func newSDKClients(t *testing.T, e *echo.Echo) sdkClients {
	t.Helper()

	inMemClient := &dashboard.InMemClient{Handler: e}

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		awscfg.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	return sdkClients{
		DDB: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String("http://local") }),
		S3: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String("http://local")
		}),
		SSM: ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) { o.BaseEndpoint = aws.String("http://local") }),
	}
}

// newLambdaHandler creates a Lambda handler backed by an in-memory backend with no Docker
// or portalloc dependency — invocations are disabled; only management-plane CRUD works.
func newLambdaHandler() *lambdabackend.Handler {
	bk := lambdabackend.NewInMemoryBackend(
		nil, nil, lambdabackend.DefaultSettings(), config.DefaultAccountID, config.DefaultRegion,
	)
	h := lambdabackend.NewHandler(bk)
	h.AccountID = config.DefaultAccountID
	h.DefaultRegion = config.DefaultRegion

	return h
}

// registerServices registers all service handlers with the registry.
func registerServices(
	registry *service.Registry,
	ddbHndlr *ddbbackend.DynamoDBHandler,
	s3Hndlr *s3backend.S3Handler,
	ssmHndlr *ssmbackend.Handler,
	iamHndlr *iambackend.Handler,
	stsHndlr *stsbackend.Handler,
	snsHndlr *snsbackend.Handler,
	sqsHndlr *sqsbackend.Handler,
	kmsHndlr *kmsbackend.Handler,
	smHndlr *smbackend.Handler,
	lambdaHndlr *lambdabackend.Handler,
	ebHndlr *ebbackend.Handler,
	apigwHndlr *apigwbackend.Handler,
	cwlogsHndlr *cwlogsbackend.Handler,
	sfnHndlr *sfnbackend.Handler,
	cwHndlr *cwbackend.Handler,
	cfnHndlr *cfnbackend.Handler,
	kinesisHndlr *kinesisbackend.Handler,
	elasticacheHndlr *elasticachebackend.Handler,
	r53Hndlr *route53backend.Handler,
	sesHndlr *sesbackend.Handler,
	sesv2Hndlr *sesv2backend.Handler,
	ec2Hndlr *ec2backend.Handler,
	ecrHndlr *ecrbackend.Handler,
	ecsHndlr *ecsbackend.Handler,
	iotHndlr *iotbackend.Handler,
	openSearchHndlr *opensearchbackend.Handler,
	acmHndlr *acmbackend.Handler,
	acmpcaHndlr *acmpcabackend.Handler,
	redshiftHndlr *redshiftbackend.Handler,
	rdsHndlr *rdsbackend.Handler,
	awsconfigHndlr *awsconfigbackend.Handler,
	s3controlHndlr *s3controlbackend.Handler,
	resourcegroupsHndlr *resourcegroupsbackend.Handler,
	rgtaHndlr *rgtabackend.Handler,
	swfHndlr *swfbackend.Handler,
	firehoseHndlr *firehosebackend.Handler,
	schedulerHndlr *schedulerbackend.Handler,
	route53resolverHndlr *route53resolverbackend.Handler,
	transcribeHndlr *transcribebackend.Handler,
	supportHndlr *supportbackend.Handler,
	cognitoIdentityHndlr *cognitoidentitybackend.Handler,
	appSyncHndlr *appsyncbackend.Handler,
	cognitoIDPHndlr *cognitoidpbackend.Handler,
	iotDataPlaneHndlr *iotdataplanebackend.Handler,
	apiGatewayMgmtHndlr *apigwmgmtbackend.Handler,
	appConfigDataHndlr *appconfigdatabackend.Handler,
	amplifyHndlr *amplifybackend.Handler,
	apigwv2Hndlr *apigwv2backend.Handler,
	appConfigHndlr *appconfigbackend.Handler,
	athenaHndlr *athenabackend.Handler,
	backupHndlr *backupbackend.Handler,
) {
	_ = registry.Register(ddbHndlr)
	_ = registry.Register(s3Hndlr)
	_ = registry.Register(ssmHndlr)
	_ = registry.Register(iamHndlr)
	_ = registry.Register(stsHndlr)
	_ = registry.Register(snsHndlr)
	_ = registry.Register(sqsHndlr)
	_ = registry.Register(kmsHndlr)
	_ = registry.Register(smHndlr)
	_ = registry.Register(lambdaHndlr)
	_ = registry.Register(ebHndlr)
	_ = registry.Register(apigwHndlr)
	_ = registry.Register(cwlogsHndlr)
	_ = registry.Register(sfnHndlr)
	_ = registry.Register(cwHndlr)
	_ = registry.Register(cfnHndlr)
	_ = registry.Register(kinesisHndlr)
	_ = registry.Register(elasticacheHndlr)
	_ = registry.Register(r53Hndlr)
	_ = registry.Register(sesHndlr)
	_ = registry.Register(sesv2Hndlr)
	_ = registry.Register(ec2Hndlr)
	_ = registry.Register(ecrHndlr)
	_ = registry.Register(ecsHndlr)
	_ = registry.Register(iotHndlr)
	_ = registry.Register(openSearchHndlr)
	_ = registry.Register(acmHndlr)
	_ = registry.Register(acmpcaHndlr)
	_ = registry.Register(redshiftHndlr)
	_ = registry.Register(rdsHndlr)
	_ = registry.Register(awsconfigHndlr)
	_ = registry.Register(s3controlHndlr)
	_ = registry.Register(resourcegroupsHndlr)
	_ = registry.Register(rgtaHndlr)
	_ = registry.Register(swfHndlr)
	_ = registry.Register(firehoseHndlr)
	_ = registry.Register(schedulerHndlr)
	_ = registry.Register(route53resolverHndlr)
	_ = registry.Register(transcribeHndlr)
	_ = registry.Register(supportHndlr)
	_ = registry.Register(cognitoIdentityHndlr)
	_ = registry.Register(appSyncHndlr)
	_ = registry.Register(cognitoIDPHndlr)
	_ = registry.Register(iotDataPlaneHndlr)
	_ = registry.Register(apiGatewayMgmtHndlr)
	_ = registry.Register(appConfigDataHndlr)
	registerExtendedServices(registry, amplifyHndlr, apigwv2Hndlr, appConfigHndlr, athenaHndlr, backupHndlr)
}

// registerExtendedServices registers service handlers added after the initial set.
func registerExtendedServices(
	registry *service.Registry,
	amplifyHndlr *amplifybackend.Handler,
	apigwv2Hndlr *apigwv2backend.Handler,
	appConfigHndlr *appconfigbackend.Handler,
	athenaHndlr *athenabackend.Handler,
	backupHndlr *backupbackend.Handler,
) {
	_ = registry.Register(amplifyHndlr)
	_ = registry.Register(apigwv2Hndlr)
	_ = registry.Register(appConfigHndlr)
	_ = registry.Register(athenaHndlr)
	_ = registry.Register(backupHndlr)
}

// registerNewestServices registers the most recently-added service handlers.
func registerNewestServices(
	registry *service.Registry,
	autoscalingHndlr *autoscalingbackend.Handler,
	appAutoScalingHndlr *applicationautoscalingbackend.Handler,
	batchHndlr *batchbackend.Handler,
	ceHndlr *cebackend.Handler,
	cloudtrailHndlr *cloudtrailbackend.Handler,
) {
	_ = registry.Register(autoscalingHndlr)
	_ = registry.Register(appAutoScalingHndlr)
	_ = registry.Register(batchHndlr)
	_ = registry.Register(ceHndlr)
	_ = registry.Register(cloudtrailHndlr)
}

// registerCloudfrontService registers the CloudFront service handler.
func registerCloudfrontService(registry *service.Registry, cloudFrontHndlr *cloudfrontbackend.Handler) {
	_ = registry.Register(cloudFrontHndlr)
}

// registerMediaServices registers the analytics and media service handlers.
func registerMediaServices(registry *service.Registry, h handlers) {
	_ = registry.Register(h.kinesisanalyticsv2)
	_ = registry.Register(h.lakeformation)
	_ = registry.Register(h.mediaconvert)
	_ = registry.Register(h.mq)
	_ = registry.Register(h.mediastore)
	_ = registry.Register(h.mediastoredata)
	_ = registry.Register(h.memorydb)
}

// registerLatestServices registers the most recently added service handlers.
func registerLatestServices(registry *service.Registry, h handlers) {
	_ = registry.Register(h.neptune)
	_ = registry.Register(h.pipes)
	_ = registry.Register(h.qldb)
}

// handlers bundles all service handlers created for a test stack.
type handlers struct {
	s3                 *s3backend.S3Handler
	ddb                *ddbbackend.DynamoDBHandler
	ssm                *ssmbackend.Handler
	iam                *iambackend.Handler
	sts                *stsbackend.Handler
	sns                *snsbackend.Handler
	sqs                *sqsbackend.Handler
	kms                *kmsbackend.Handler
	sm                 *smbackend.Handler
	lambda             *lambdabackend.Handler
	eb                 *ebbackend.Handler
	apigw              *apigwbackend.Handler
	cwlogs             *cwlogsbackend.Handler
	sfn                *sfnbackend.Handler
	cw                 *cwbackend.Handler
	cfn                *cfnbackend.Handler
	kinesis            *kinesisbackend.Handler
	elasticache        *elasticachebackend.Handler
	route53            *route53backend.Handler
	ses                *sesbackend.Handler
	sesv2              *sesv2backend.Handler
	ec2                *ec2backend.Handler
	ecr                *ecrbackend.Handler
	ecs                *ecsbackend.Handler
	iot                *iotbackend.Handler
	fis                *fisbackend.Handler
	identitystore      *identitystorebackend.Handler
	opensearch         *opensearchbackend.Handler
	acm                *acmbackend.Handler
	acmpca             *acmpcabackend.Handler
	redshift           *redshiftbackend.Handler
	rds                *rdsbackend.Handler
	docdb              *docdbbackend.Handler
	awsconfig          *awsconfigbackend.Handler
	s3control          *s3controlbackend.Handler
	resourcegroups     *resourcegroupsbackend.Handler
	rgtagging          *rgtabackend.Handler
	swf                *swfbackend.Handler
	firehose           *firehosebackend.Handler
	scheduler          *schedulerbackend.Handler
	route53resolver    *route53resolverbackend.Handler
	transcribe         *transcribebackend.Handler
	support            *supportbackend.Handler
	cognitoIdentity    *cognitoidentitybackend.Handler
	appSync            *appsyncbackend.Handler
	cognitoIDP         *cognitoidpbackend.Handler
	iotDataPlane       *iotdataplanebackend.Handler
	apiGatewayMgmt     *apigwmgmtbackend.Handler
	appConfigData      *appconfigdatabackend.Handler
	amplify            *amplifybackend.Handler
	apigwv2            *apigwv2backend.Handler
	appConfig          *appconfigbackend.Handler
	athena             *athenabackend.Handler
	autoscaling        *autoscalingbackend.Handler
	appAutoScaling     *applicationautoscalingbackend.Handler
	backup             *backupbackend.Handler
	cloudtrail         *cloudtrailbackend.Handler
	batch              *batchbackend.Handler
	bedrock            *bedrockbackend.Handler
	bedrockruntime     *bedrockruntimebackend.Handler
	ce                 *cebackend.Handler
	cloudcontrol       *cloudcontrolbackend.Handler
	cloudFront         *cloudfrontbackend.Handler
	codeArtifact       *codeartifactbackend.Handler
	codebuild          *codebuildbackend.Handler
	codeCommit         *codecommitbackend.Handler
	codePipeline       *codepipelinebackend.Handler
	codeConnections    *codeconnectionsbackend.Handler
	codeDeploy         *codedeploybackend.Handler
	dms                *dmsbackend.Handler
	codeStarConn       *codestarconnectionsbackend.Handler
	dynamodbStreams    *dynamodbstreamsbackend.Handler
	elasticbeanstalk   *elasticbeanstalkbackend.Handler
	elastictranscoder  *elastictranscoderbackend.Handler
	efs                *efsbackend.Handler
	eks                *eksbackend.Handler
	elb                *elbbackend.Handler
	elbv2              *elbv2backend.Handler
	emrserverless      *emrserverlessbackend.Handler
	emr                *emrbackend.Handler
	glacier            *glacierbackend.Handler
	iotanalytics       *iotanalyticsbackend.Handler
	iotwireless        *iotwirelessbackend.Handler
	kinesisanalytics   *kinesisanalyticsbackend.Handler
	kafka              *kafkabackend.Handler
	kinesisanalyticsv2 *kinesisanalyticsv2backend.Handler
	lakeformation      *lakeformationbackend.Handler
	mediaconvert       *mediaconvertbackend.Handler
	mq                 *mqbackend.Handler
	mediastore         *mediastorebackend.Handler
	mediastoredata     *mediastoredatabackend.Handler
	memorydb           *memorydbbackend.Handler
	mwaa               *mwaabackend.Handler
	neptune            *neptunebackend.Handler
	pipes              *pipesbackend.Handler
	qldb               *qldbbackend.Handler
	iamBk              *iambackend.InMemoryBackend
	s3Bk               *s3backend.InMemoryBackend
}

// newHandlers creates in-memory backends and handlers for all services.
func newHandlers() handlers {
	s3Bk := s3backend.NewInMemoryBackend(nil)
	iamBk := iambackend.NewInMemoryBackend()
	ddb := ddbbackend.NewHandler(ddbbackend.NewInMemoryDB())
	sqs := sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend())
	sns := snsbackend.NewHandler(snsbackend.NewInMemoryBackend())
	ssm := ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend())
	kms := kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend())
	sm := smbackend.NewHandler(smbackend.NewInMemoryBackend())

	h := handlers{
		s3Bk:    s3Bk,
		iamBk:   iamBk,
		s3:      s3backend.NewHandler(s3Bk),
		ddb:     ddb,
		ssm:     ssm,
		iam:     iambackend.NewHandler(iamBk),
		sts:     stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
		sns:     sns,
		sqs:     sqs,
		kms:     kms,
		sm:      sm,
		lambda:  newLambdaHandler(),
		eb:      ebbackend.NewHandler(ebbackend.NewInMemoryBackend()),
		apigw:   apigwbackend.NewHandler(apigwbackend.NewInMemoryBackend()),
		cwlogs:  cwlogsbackend.NewHandler(cwlogsbackend.NewInMemoryBackend()),
		sfn:     sfnbackend.NewHandler(sfnbackend.NewInMemoryBackend()),
		cw:      cwbackend.NewHandler(cwbackend.NewInMemoryBackend()),
		cfn:     newCFNHandler(s3Bk, ddb, sqs, sns, ssm, kms, sm),
		kinesis: kinesisbackend.NewHandler(kinesisbackend.NewInMemoryBackend()),
		elasticache: elasticachebackend.NewHandler(
			elasticachebackend.NewInMemoryBackend(
				elasticachebackend.EngineStub, config.DefaultAccountID, config.DefaultRegion,
			),
		),
		route53: route53backend.NewHandler(route53backend.NewInMemoryBackend()),
		ses:     sesbackend.NewHandler(sesbackend.NewInMemoryBackend()),
		sesv2:   sesv2backend.NewHandler(sesv2backend.NewInMemoryBackend()),
	}
	populateExtendedHandlers(&h)

	return h
}

// populateExtendedHandlers fills in the regional and newer service handlers that would push
// newHandlers past the funlen limit.
func populateExtendedHandlers(h *handlers) {
	h.ec2 = ec2backend.NewHandler(
		ec2backend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.ecr = ecrbackend.NewHandler(
		ecrbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion, ""),
		nil,
	)
	h.ecs = ecsbackend.NewHandler(
		ecsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion, ecsbackend.NewNoopRunner()),
	)
	h.iot = iotbackend.NewHandler(
		iotbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion),
		nil, // broker is nil in tests; MQTT publish/subscribe is not exercised by dashboard tests
	)
	h.fis = fisbackend.NewHandler(
		fisbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.identitystore = identitystorebackend.NewHandler(
		identitystorebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.opensearch = opensearchbackend.NewHandler(
		opensearchbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.acm = acmbackend.NewHandler(
		acmbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.acmpca = acmpcabackend.NewHandler(
		acmpcabackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.redshift = redshiftbackend.NewHandler(
		redshiftbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.rds = rdsbackend.NewHandler(
		rdsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.docdb = docdbbackend.NewHandler(
		docdbbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.awsconfig = awsconfigbackend.NewHandler(awsconfigbackend.NewInMemoryBackend())
	h.s3control = s3controlbackend.NewHandler(s3controlbackend.NewInMemoryBackend())
	h.resourcegroups = resourcegroupsbackend.NewHandler(
		resourcegroupsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.rgtagging = rgtabackend.NewHandler(
		rgtabackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.swf = swfbackend.NewHandler(swfbackend.NewInMemoryBackend())
	h.firehose = firehosebackend.NewHandler(
		firehosebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.scheduler = schedulerbackend.NewHandler(
		schedulerbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.route53resolver = route53resolverbackend.NewHandler(
		route53resolverbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.transcribe = transcribebackend.NewHandler(transcribebackend.NewInMemoryBackend())
	h.support = supportbackend.NewHandler(supportbackend.NewInMemoryBackend())

	populateNewestHandlers(h)
}

// populateNewestHandlers fills in the most recently added service handlers that would push
// populateExtendedHandlers past the funlen limit.
func populateNewestHandlers(h *handlers) {
	h.cognitoIdentity = cognitoidentitybackend.NewHandler(
		cognitoidentitybackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		config.DefaultRegion,
	)
	h.appSync = appsyncbackend.NewHandler(
		appsyncbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion, "http://localhost:8000"),
	)
	h.cognitoIDP = cognitoidpbackend.NewHandler(
		cognitoidpbackend.NewInMemoryBackend(
			config.DefaultAccountID,
			config.DefaultRegion,
			"http://localhost:8000",
		),
		config.DefaultRegion,
	)
	h.iotDataPlane = iotdataplanebackend.NewHandler(iotdataplanebackend.NewInMemoryBackend())
	h.apiGatewayMgmt = apigwmgmtbackend.NewHandler(apigwmgmtbackend.NewInMemoryBackend())
	h.appConfigData = appconfigdatabackend.NewHandler(appconfigdatabackend.NewInMemoryBackend())
	h.amplify = amplifybackend.NewHandler(
		amplifybackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.apigwv2 = apigwv2backend.NewHandler(apigwv2backend.NewInMemoryBackend())
	h.appConfig = appconfigbackend.NewHandler(appconfigbackend.NewInMemoryBackend())
	h.athena = athenabackend.NewHandler(athenabackend.NewInMemoryBackend())
	h.autoscaling = autoscalingbackend.NewHandler(autoscalingbackend.NewInMemoryBackend())
	h.appAutoScaling = applicationautoscalingbackend.NewHandler(
		applicationautoscalingbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.backup = backupbackend.NewHandler(
		backupbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.cloudtrail = cloudtrailbackend.NewHandler(
		cloudtrailbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.batch = batchbackend.NewHandler(batchbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	h.bedrock = bedrockbackend.NewHandler(
		bedrockbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.bedrockruntime = bedrockruntimebackend.NewHandler(
		bedrockruntimebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.ce = cebackend.NewHandler(cebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	h.cloudcontrol = cloudcontrolbackend.NewHandler(
		cloudcontrolbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.cloudFront = cloudfrontbackend.NewHandler(
		cloudfrontbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codeArtifact = codeartifactbackend.NewHandler(
		codeartifactbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codebuild = codebuildbackend.NewHandler(
		codebuildbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codeCommit = codecommitbackend.NewHandler(
		codecommitbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codePipeline = codepipelinebackend.NewHandler(
		codepipelinebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codeConnections = codeconnectionsbackend.NewHandler(
		codeconnectionsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codeDeploy = codedeploybackend.NewHandler(
		codedeploybackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.dms = dmsbackend.NewHandler(
		dmsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.codeStarConn = codestarconnectionsbackend.NewHandler(
		codestarconnectionsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	if ddbBk, ok := h.ddb.Backend.(ddbbackend.StreamsBackend); ok {
		h.dynamodbStreams = dynamodbstreamsbackend.NewHandler(ddbBk)
	}

	h.elasticbeanstalk = elasticbeanstalkbackend.NewHandler(
		elasticbeanstalkbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.efs = efsbackend.NewHandler(
		efsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.eks = eksbackend.NewHandler(
		eksbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.elastictranscoder = elastictranscoderbackend.NewHandler(
		elastictranscoderbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.elb = elbbackend.NewHandler(
		elbbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	populateLatestHandlers(h)
}

// populateLatestHandlers fills in the most recently added service handlers that would push
// populateNewestHandlers past the funlen limit.
// populateLatestHandlers fills in the most recently added service handlers that would push
// populateNewestHandlers past the funlen limit.
func populateLatestHandlers(h *handlers) {
	h.elbv2 = elbv2backend.NewHandler(
		elbv2backend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.emrserverless = emrserverlessbackend.NewHandler(
		emrserverlessbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.emr = emrbackend.NewHandler(
		emrbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.glacier = glacierbackend.NewHandler(glacierbackend.NewInMemoryBackend())
	h.glacier.AccountID = config.DefaultAccountID
	h.glacier.DefaultRegion = config.DefaultRegion

	h.iotanalytics = iotanalyticsbackend.NewHandler(iotanalyticsbackend.NewInMemoryBackend())

	h.iotwireless = iotwirelessbackend.NewHandler(iotwirelessbackend.NewInMemoryBackend())
	h.iotwireless.AccountID = config.DefaultAccountID
	h.iotwireless.DefaultRegion = config.DefaultRegion

	h.kinesisanalytics = kinesisanalyticsbackend.NewHandler(
		kinesisanalyticsbackend.NewInMemoryBackend(config.DefaultRegion, config.DefaultAccountID),
	)
	h.kinesisanalytics.AccountID = config.DefaultAccountID
	h.kinesisanalytics.DefaultRegion = config.DefaultRegion

	h.kafka = kafkabackend.NewHandler(
		kafkabackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.kinesisanalyticsv2 = kinesisanalyticsv2backend.NewHandler(
		kinesisanalyticsv2backend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)

	h.lakeformation = lakeformationbackend.NewHandler(lakeformationbackend.NewInMemoryBackend())
	h.lakeformation.AccountID = config.DefaultAccountID
	h.lakeformation.DefaultRegion = config.DefaultRegion

	h.mediaconvert = mediaconvertbackend.NewHandler(
		mediaconvertbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.mq = mqbackend.NewHandler(
		mqbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.mediastore = mediastorebackend.NewHandler(mediastorebackend.NewInMemoryBackend())
	h.mediastore.AccountID = config.DefaultAccountID
	h.mediastore.DefaultRegion = config.DefaultRegion

	h.mediastoredata = mediastoredatabackend.NewHandler(mediastoredatabackend.NewInMemoryBackend())
	h.memorydb = memorydbbackend.NewHandler(memorydbbackend.NewInMemoryBackend())
	h.memorydb.AccountID = config.DefaultAccountID
	h.memorydb.DefaultRegion = config.DefaultRegion

	h.mwaa = mwaabackend.NewHandler(mwaabackend.NewInMemoryBackend(config.DefaultRegion, config.DefaultAccountID))
	h.mwaa.AccountID = config.DefaultAccountID
	h.mwaa.DefaultRegion = config.DefaultRegion
	h.neptune = neptunebackend.NewHandler(
		neptunebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	)
	h.pipes = pipesbackend.NewHandler(pipesbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	h.qldb = qldbbackend.NewHandler(qldbbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
}

// newCFNHandler creates a CloudFormation handler wired to the given service backends
// so that CreateStack actually provisions real resources.
func newCFNHandler(
	s3Bk *s3backend.InMemoryBackend,
	ddb *ddbbackend.DynamoDBHandler,
	sqs *sqsbackend.Handler,
	sns *snsbackend.Handler,
	ssm *ssmbackend.Handler,
	kms *kmsbackend.Handler,
	sm *smbackend.Handler,
) *cfnbackend.Handler {
	backends := &cfnbackend.ServiceBackends{
		S3:             s3backend.NewHandler(s3Bk),
		DynamoDB:       ddb,
		SQS:            sqs,
		SNS:            sns,
		SSM:            ssm,
		KMS:            kms,
		SecretsManager: sm,
		AccountID:      config.DefaultAccountID,
		Region:         config.DefaultRegion,
	}
	creator := cfnbackend.NewResourceCreator(backends)
	backend := cfnbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion, creator)

	return cfnbackend.NewHandler(backend)
}

// newDashboardConfig builds the dashboard.Config for the test stack.
func newDashboardConfig(h handlers, clients sdkClients) (dashboard.Config, *chaos.FaultStore) {
	fs := chaos.NewFaultStore()
	gc := config.GlobalConfig{AccountID: config.DefaultAccountID, Region: config.DefaultRegion}

	cfg := dashboard.Config{
		DDBClient:                  clients.DDB,
		S3Client:                   clients.S3,
		SSMClient:                  clients.SSM,
		DDBOps:                     h.ddb,
		S3Ops:                      h.s3,
		SSMOps:                     h.ssm,
		IAMOps:                     h.iam,
		STSOps:                     h.sts,
		SNSOps:                     h.sns,
		SQSOps:                     h.sqs,
		KMSOps:                     h.kms,
		SecretsManagerOps:          h.sm,
		LambdaOps:                  h.lambda,
		EventBridgeOps:             h.eb,
		APIGatewayOps:              h.apigw,
		CloudWatchLogsOps:          h.cwlogs,
		StepFunctionsOps:           h.sfn,
		CloudWatchOps:              h.cw,
		CloudFormationOps:          h.cfn,
		KinesisOps:                 h.kinesis,
		ElastiCacheOps:             h.elasticache,
		Route53Ops:                 h.route53,
		SESOps:                     h.ses,
		SESv2Ops:                   h.sesv2,
		EC2Ops:                     h.ec2,
		ECROps:                     h.ecr,
		ECSOps:                     h.ecs,
		IoTOps:                     h.iot,
		FISOps:                     h.fis,
		IdentityStoreOps:           h.identitystore,
		OpenSearchOps:              h.opensearch,
		ACMOps:                     h.acm,
		ACMPCAOps:                  h.acmpca,
		RedshiftOps:                h.redshift,
		RDSOps:                     h.rds,
		DocDBOps:                   h.docdb,
		AWSConfigOps:               h.awsconfig,
		S3ControlOps:               h.s3control,
		ResourceGroupsOps:          h.resourcegroups,
		ResourceGroupsTaggingOps:   h.rgtagging,
		SWFOps:                     h.swf,
		FirehoseOps:                h.firehose,
		SchedulerOps:               h.scheduler,
		Route53ResolverOps:         h.route53resolver,
		TranscribeOps:              h.transcribe,
		SupportOps:                 h.support,
		CognitoIdentityOps:         h.cognitoIdentity,
		AppSyncOps:                 h.appSync,
		CognitoIDPOps:              h.cognitoIDP,
		IoTDataPlaneOps:            h.iotDataPlane,
		APIGatewayManagementAPIOps: h.apiGatewayMgmt,
		AppConfigDataOps:           h.appConfigData,
		AmplifyOps:                 h.amplify,
		APIGatewayV2Ops:            h.apigwv2,
		AppConfigOps:               h.appConfig,
		AthenaOps:                  h.athena,
		AutoscalingOps:             h.autoscaling,
		ApplicationAutoscalingOps:  h.appAutoScaling,
		BackupOps:                  h.backup,
		CloudTrailOps:              h.cloudtrail,
		BatchOps:                   h.batch,
		BedrockOps:                 h.bedrock,
		BedrockRuntimeOps:          h.bedrockruntime,
		CeOps:                      h.ce,
		CloudControlOps:            h.cloudcontrol,
		CloudFrontOps:              h.cloudFront,
		CodeArtifactOps:            h.codeArtifact,
		CodeBuildOps:               h.codebuild,
		CodeCommitOps:              h.codeCommit,
		CodePipelineOps:            h.codePipeline,
		CodeConnectionsOps:         h.codeConnections,
		CodeDeployOps:              h.codeDeploy,
		DMSOps:                     h.dms,
		CodeStarConnectionsOps:     h.codeStarConn,
		DynamoDBStreamsOps:         h.dynamodbStreams,
		ElasticbeanstalkOps:        h.elasticbeanstalk,
		EFSOps:                     h.efs,
		EKSOps:                     h.eks,
		ElasticTranscoderOps:       h.elastictranscoder,
		ELBOps:                     h.elb,
		ELBv2Ops:                   h.elbv2,
		EmrServerlessOps:           h.emrserverless,
		EMROps:                     h.emr,
		GlacierOps:                 h.glacier,
		IoTAnalyticsOps:            h.iotanalytics,
		IoTWirelessOps:             h.iotwireless,
		KinesisAnalyticsOps:        h.kinesisanalytics,
		KafkaOps:                   h.kafka,
		GlobalConfig:               gc,
		FaultStore:                 fs,
		Logger:                     slog.Default(),
	}

	applyNewestDashboardOps(&cfg, h)

	return cfg, fs
}

// applyNewestDashboardOps sets the most recently added service ops on a dashboard.Config.
// It is extracted from newDashboardConfig to satisfy the funlen limit.
func applyNewestDashboardOps(cfg *dashboard.Config, h handlers) {
	cfg.EmrServerlessOps = h.emrserverless
	cfg.EMROps = h.emr
	cfg.GlacierOps = h.glacier
	cfg.IoTAnalyticsOps = h.iotanalytics
	cfg.IoTWirelessOps = h.iotwireless
	cfg.KinesisAnalyticsOps = h.kinesisanalytics
	cfg.KafkaOps = h.kafka
	cfg.KinesisAnalyticsV2Ops = h.kinesisanalyticsv2
	cfg.LakeFormationOps = h.lakeformation
	cfg.MediaConvertOps = h.mediaconvert
	cfg.MQOps = h.mq
	cfg.MediaStoreOps = h.mediastore
	cfg.MediaStoreDataOps = h.mediastoredata
	cfg.MemoryDBOps = h.memorydb
	cfg.MWAAOps = h.mwaa
	cfg.NeptuneOps = h.neptune
	cfg.PipesOps = h.pipes
	cfg.QLDBOps = h.qldb
}

// New creates a fully wired integration stack for testing.
// It sets up all in-memory backends, handlers, the service registry with router,
// AWS SDK clients (routed back through Echo via InMemClient), and the dashboard.
func New(t *testing.T) *Stack {
	t.Helper()

	testLogger := logger.NewTestLogger()
	h := newHandlers()

	// Set up Echo with service registry and router.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(testLogger))

	registry := service.NewRegistry()
	registerServices(
		registry,
		h.ddb, h.s3, h.ssm, h.iam, h.sts, h.sns, h.sqs, h.kms, h.sm,
		h.lambda, h.eb, h.apigw, h.cwlogs, h.sfn, h.cw, h.cfn, h.kinesis,
		h.elasticache, h.route53, h.ses, h.sesv2, h.ec2, h.ecr, h.ecs, h.iot, h.opensearch,
		h.acm, h.acmpca, h.redshift, h.rds, h.awsconfig, h.s3control, h.resourcegroups, h.rgtagging, h.swf, h.firehose,
		h.scheduler, h.route53resolver, h.transcribe, h.support, h.cognitoIdentity,
		h.appSync, h.cognitoIDP, h.iotDataPlane, h.apiGatewayMgmt, h.appConfigData,
		h.amplify, h.apigwv2, h.appConfig, h.athena, h.backup,
	)
	registerNewestServices(registry, h.autoscaling, h.appAutoScaling, h.batch, h.ce, h.cloudtrail)
	_ = registry.Register(h.docdb)
	_ = registry.Register(h.bedrock)
	_ = registry.Register(h.bedrockruntime)
	_ = registry.Register(h.cloudcontrol)
	registerCloudfrontService(registry, h.cloudFront)
	_ = registry.Register(h.codeArtifact)
	_ = registry.Register(h.codebuild)
	_ = registry.Register(h.codeCommit)
	_ = registry.Register(h.codePipeline)
	_ = registry.Register(h.codeConnections)
	_ = registry.Register(h.codeDeploy)
	_ = registry.Register(h.dms)
	_ = registry.Register(h.codeStarConn)

	if h.dynamodbStreams != nil {
		_ = registry.Register(h.dynamodbStreams)
	}

	_ = registry.Register(h.elasticbeanstalk)
	_ = registry.Register(h.efs)
	_ = registry.Register(h.eks)
	_ = registry.Register(h.elastictranscoder)
	_ = registry.Register(h.elb)
	_ = registry.Register(h.elbv2)
	_ = registry.Register(h.emrserverless)
	_ = registry.Register(h.emr)
	_ = registry.Register(h.fis)
	_ = registry.Register(h.identitystore)
	_ = registry.Register(h.glacier)
	_ = registry.Register(h.iotanalytics)
	_ = registry.Register(h.iotwireless)
	_ = registry.Register(h.kinesisanalytics)
	_ = registry.Register(h.kafka)
	_ = registry.Register(h.mwaa)
	registerMediaServices(registry, h)
	registerLatestServices(registry, h)

	// Create AWS SDK clients routed through in-memory Echo, then wire dashboard.
	clients := newSDKClients(t, e)
	dashCfg, faultStore := newDashboardConfig(h, clients)
	dashHndlr := dashboard.NewHandler(dashCfg)
	_ = registry.Register(dashHndlr)

	// Mount the service router — this is the step that was previously easy to forget.
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	s := buildStack(e, h, clients, faultStore, dashHndlr)

	return s
}

// buildStack assembles the Stack struct from wired components.
// It is extracted from New to satisfy the funlen limit on that function.
func buildStack(
	e *echo.Echo,
	h handlers,
	clients sdkClients,
	faultStore *chaos.FaultStore,
	dashboardHandler *dashboard.DashboardHandler,
) *Stack {
	s := &Stack{
		Echo:                           e,
		S3Backend:                      h.s3Bk,
		S3Handler:                      h.s3,
		DDBHandler:                     h.ddb,
		IAMBackend:                     h.iamBk,
		IAMHandler:                     h.iam,
		STSHandler:                     h.sts,
		SNSHandler:                     h.sns,
		SQSHandler:                     h.sqs,
		KMSHandler:                     h.kms,
		SecretsManagerHandler:          h.sm,
		LambdaHandler:                  h.lambda,
		EventBridgeHandler:             h.eb,
		APIGatewayHandler:              h.apigw,
		CloudWatchLogsHandler:          h.cwlogs,
		StepFunctionsHandler:           h.sfn,
		CloudWatchHandler:              h.cw,
		CloudFormationHandler:          h.cfn,
		KinesisHandler:                 h.kinesis,
		ElastiCacheHandler:             h.elasticache,
		Route53Handler:                 h.route53,
		SESHandler:                     h.ses,
		SESv2Handler:                   h.sesv2,
		EC2Handler:                     h.ec2,
		ECRHandler:                     h.ecr,
		ECSHandler:                     h.ecs,
		IoTHandler:                     h.iot,
		FISHandler:                     h.fis,
		IdentityStoreHandler:           h.identitystore,
		OpenSearchHandler:              h.opensearch,
		ACMHandler:                     h.acm,
		ACMPCAHandler:                  h.acmpca,
		RedshiftHandler:                h.redshift,
		RDSHandler:                     h.rds,
		DocDBHandler:                   h.docdb,
		AWSConfigHandler:               h.awsconfig,
		S3ControlHandler:               h.s3control,
		ResourceGroupsHandler:          h.resourcegroups,
		ResourceGroupsTaggingHandler:   h.rgtagging,
		SWFHandler:                     h.swf,
		FirehoseHandler:                h.firehose,
		SchedulerHandler:               h.scheduler,
		Route53ResolverHandler:         h.route53resolver,
		TranscribeHandler:              h.transcribe,
		SupportHandler:                 h.support,
		CognitoIdentityHandler:         h.cognitoIdentity,
		AppSyncHandler:                 h.appSync,
		CognitoIDPHandler:              h.cognitoIDP,
		IoTDataPlaneHandler:            h.iotDataPlane,
		APIGatewayManagementAPIHandler: h.apiGatewayMgmt,
		AppConfigDataHandler:           h.appConfigData,
		AmplifyHandler:                 h.amplify,
		APIGatewayV2Handler:            h.apigwv2,
		AppConfigHandler:               h.appConfig,
		AthenaHandler:                  h.athena,
		AutoscalingHandler:             h.autoscaling,
		ApplicationAutoscalingHandler:  h.appAutoScaling,
		BackupHandler:                  h.backup,
		CloudTrailHandler:              h.cloudtrail,
		BatchHandler:                   h.batch,
		BedrockHandler:                 h.bedrock,
		BedrockRuntimeHandler:          h.bedrockruntime,
		CeHandler:                      h.ce,
		CloudControlHandler:            h.cloudcontrol,
		CloudFrontHandler:              h.cloudFront,
		CodeArtifactHandler:            h.codeArtifact,
		CodeBuildHandler:               h.codebuild,
		CodeCommitHandler:              h.codeCommit,
		CodePipelineHandler:            h.codePipeline,
		CodeConnectionsHandler:         h.codeConnections,
		CodeDeployHandler:              h.codeDeploy,
		DMSHandler:                     h.dms,
		CodeStarConnectionsHandler:     h.codeStarConn,
		DynamoDBStreamsHandler:         h.dynamodbStreams,
		ElasticbeanstalkHandler:        h.elasticbeanstalk,
		EFSHandler:                     h.efs,
		EKSHandler:                     h.eks,
		ElasticTranscoderHandler:       h.elastictranscoder,
		ELBHandler:                     h.elb,
		ELBv2Handler:                   h.elbv2,
		GlacierHandler:                 h.glacier,
		IoTAnalyticsHandler:            h.iotanalytics,
		IoTWirelessHandler:             h.iotwireless,
		KinesisAnalyticsHandler:        h.kinesisanalytics,
		KafkaHandler:                   h.kafka,
		S3Client:                       clients.S3,
		DDBClient:                      clients.DDB,
		FaultStore:                     faultStore,
		Dashboard:                      dashboardHandler,
	}
	setNewestStackHandlers(s, h)

	return s
}

// setNewestStackHandlers sets the most recently added handler fields on a Stack.
// It is extracted from buildStack to satisfy the funlen limit.
func setNewestStackHandlers(s *Stack, h handlers) {
	s.EmrServerlessHandler = h.emrserverless
	s.EMRHandler = h.emr
	s.KinesisAnalyticsHandler = h.kinesisanalytics
	s.KafkaHandler = h.kafka
	s.KinesisAnalyticsV2Handler = h.kinesisanalyticsv2
	s.LakeFormationHandler = h.lakeformation
	s.MediaConvertHandler = h.mediaconvert
	s.MQHandler = h.mq
	s.MediaStoreHandler = h.mediastore
	s.MediaStoreDataHandler = h.mediastoredata
	s.MemoryDBHandler = h.memorydb
	s.MWAAHandler = h.mwaa
	s.NeptuneHandler = h.neptune
	s.PipesHandler = h.pipes
	s.QLDBHandler = h.qldb
}

// CreateDDBTable creates a DynamoDB table with a simple string hash key "id".
func (s *Stack) CreateDDBTable(t *testing.T, tableName string) {
	t.Helper()

	_, err := s.DDBHandler.Backend.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(testReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(testWriteCapacityUnits),
		},
	})
	require.NoError(t, err)
}

// CreateS3Bucket creates an S3 bucket with the given name.
func (s *Stack) CreateS3Bucket(t *testing.T, bucketName string) {
	t.Helper()

	_, err := s.S3Backend.CreateBucket(
		t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	require.NoError(t, err)
}
