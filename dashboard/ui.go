package dashboard

import (
	"embed"
	"encoding/json"
	"html"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/dashboard/api/v1/dashboardv1connect"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
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
	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
	elastictranscoderbackend "github.com/blackbirdworks/gopherstack/services/elastictranscoder"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	emrserverlessbackend "github.com/blackbirdworks/gopherstack/services/emrserverless"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	glacierbackend "github.com/blackbirdworks/gopherstack/services/glacier"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
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
	managedblockchainbackend "github.com/blackbirdworks/gopherstack/services/managedblockchain"
	mediaconvertbackend "github.com/blackbirdworks/gopherstack/services/mediaconvert"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	mediastoredatabackend "github.com/blackbirdworks/gopherstack/services/mediastoredata"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	mwaabackend "github.com/blackbirdworks/gopherstack/services/mwaa"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	pinpointbackend "github.com/blackbirdworks/gopherstack/services/pinpoint"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	qldbbackend "github.com/blackbirdworks/gopherstack/services/qldb"
	qldbsessionbackend "github.com/blackbirdworks/gopherstack/services/qldbsession"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	rdsdatabackend "github.com/blackbirdworks/gopherstack/services/rdsdata"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	taggingbackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
	s3tablesbackend "github.com/blackbirdworks/gopherstack/services/s3tables"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	sagemakerruntimebackend "github.com/blackbirdworks/gopherstack/services/sagemakerrumtime"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	serverlessrepobackend "github.com/blackbirdworks/gopherstack/services/serverlessrepo"
	servicediscoverybackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	sesv2backend "github.com/blackbirdworks/gopherstack/services/sesv2"
	shieldbackend "github.com/blackbirdworks/gopherstack/services/shield"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	ssoadminbackend "github.com/blackbirdworks/gopherstack/services/ssoadmin"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	textractbackend "github.com/blackbirdworks/gopherstack/services/textract"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
	verifiedpermissionsbackend "github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"
	xraybackend "github.com/blackbirdworks/gopherstack/services/xray"
)

// S3Settings holds dashboard-specific S3 configuration.
type S3Settings struct {
	DefaultRegion       string
	JanitorInterval     time.Duration
	CompressionMinBytes int
}

// LambdaSettings holds dashboard-specific Lambda configuration.
type LambdaSettings struct {
	DockerHost       string
	ContainerRuntime string
	PoolSize         int
	IdleTimeout      time.Duration
	MaxRuntimes      int
}

// DynamoDBSettings holds dashboard-specific DynamoDB configuration.
type DynamoDBSettings struct {
	DefaultRegion     string
	JanitorInterval   time.Duration
	CreateDelay       time.Duration
	EnforceThroughput bool
}

// EC2Settings holds dashboard-specific EC2 configuration.
type EC2Settings struct {
	JanitorInterval  time.Duration
	TerminatedTTL    time.Duration
	CancelledSpotTTL time.Duration
}

// BackupSettings holds dashboard-specific Backup configuration.
type BackupSettings struct {
	JanitorInterval time.Duration
	JobTTL          time.Duration
}

// STSSettings holds dashboard-specific STS configuration.
type STSSettings struct {
	JanitorInterval time.Duration
}

// XRaySettings holds dashboard-specific X-Ray configuration.
type XRaySettings struct {
	JanitorInterval time.Duration
	TraceTTL        time.Duration
}

// SSMSettings holds dashboard-specific SSM configuration.
type SSMSettings struct {
	JanitorInterval time.Duration
	CommandTTL      time.Duration
}

// CodeBuildSettings holds dashboard-specific CodeBuild configuration.
type CodeBuildSettings struct {
	JanitorInterval time.Duration
	BuildTTL        time.Duration
}

// CloudWatchLogsSettings holds dashboard-specific CloudWatch Logs configuration.
type CloudWatchLogsSettings struct {
	JanitorInterval time.Duration
}

// SESSettings holds dashboard-specific SES configuration.
type SESSettings struct {
	JanitorInterval time.Duration
	EmailTTL        time.Duration
}

// BatchSettings holds dashboard-specific Batch configuration.
type BatchSettings struct {
	JanitorInterval   time.Duration
	InactiveJobDefTTL time.Duration
	CompletedJobTTL   time.Duration
}

// FISSettings holds dashboard-specific FIS configuration.
type FISSettings struct {
	JanitorInterval time.Duration
	ExperimentTTL   time.Duration
}

// EMRSettings holds dashboard-specific EMR configuration.
type EMRSettings struct {
	JanitorInterval time.Duration
	TerminatedTTL   time.Duration
}

// AthenaSettings holds dashboard-specific Athena configuration.
type AthenaSettings struct {
	JanitorInterval time.Duration
	ExecutionTTL    time.Duration
}

// KinesisSettings holds dashboard-specific Kinesis configuration.
type KinesisSettings struct {
	JanitorInterval time.Duration
}

// KMSSettings holds dashboard-specific KMS configuration.
type KMSSettings struct {
	JanitorInterval time.Duration
}

// Settings holds the overall dashboard internal state/preferences.
type Settings struct {
	LogLevel            string
	Port                string
	ElastiCacheEngine   string
	AccountID           string
	ElasticsearchEngine string
	DNSListenAddr       string
	Region              string
	OpenSearchEngine    string
	DataDir             string
	DNSResolveIP        string
	S3                  S3Settings
	Lambda              LambdaSettings
	DynamoDB            DynamoDBSettings
	EC2                 EC2Settings
	Batch               BatchSettings
	CodeBuild           CodeBuildSettings
	FIS                 FISSettings
	Athena              AthenaSettings
	EMR                 EMRSettings
	SES                 SESSettings
	SSM                 SSMSettings
	XRay                XRaySettings
	Backup              BackupSettings
	PortRangeEnd        int
	STS                 STSSettings
	JanitorTimeout      time.Duration
	AutoPurgeTTL        time.Duration
	KMS                 KMSSettings
	CloudWatchLogs      CloudWatchLogsSettings
	Kinesis             KinesisSettings
	LatencyMs           int
	PortRangeStart      int
	InitScriptTimeout   time.Duration
	Persist             bool
	Demo                bool
	EnforceIAM          bool
}

// ConfigManager defines an interface for saving server configuration.
type ConfigManager interface {
	GetSettings() Settings
	UpdateSettings(Settings)
	SaveConfig() error
}

// AWSSDKProvider defines an interface for providing access to AWS service handlers and clients.
// This is used by the dashboard provider to discover and link service backends.
type AWSSDKProvider interface {
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
	GetECRHandler() service.Registerable
	GetECSHandler() service.Registerable
	GetEFSHandler() service.Registerable
	GetEKSHandler() service.Registerable
	GetIoTHandler() service.Registerable
	GetAPIGatewayManagementAPIHandler() service.Registerable
	GetAppConfigDataHandler() service.Registerable
	GetAmplifyHandler() service.Registerable
	GetAPIGatewayV2Handler() service.Registerable
	GetAppConfigHandler() service.Registerable
	GetAthenaHandler() service.Registerable
	GetAutoscalingHandler() service.Registerable
	GetBackupHandler() service.Registerable
	GetCloudTrailHandler() service.Registerable
}

//go:embed all:static
var staticFS embed.FS

//go:embed all:static/spa
var spaFS embed.FS

// DashboardHandler handles HTTP requests for the Dashboard web interface.
//
//nolint:revive // keep exported name stable for existing consumers.
type DashboardHandler struct {
	FaultStore    *chaos.FaultStore
	ConfigManager ConfigManager
	GlobalConfig  *config.GlobalConfig
	Logger        *slog.Logger
	SubRouter     *echo.Echo
	config        Config
	mu            sync.RWMutex
}

// Config is the configuration for the Dashboard service.
// This struct is preserved with all its original fields for CLI and TestStack compatibility,
// even though most fields are no longer used by the new SPA-based DashboardHandler.
type Config struct {
	DDBClient *dynamodb.Client
	S3Client  *s3.Client
	SSMClient *ssmsdk.Client
	DDBOps    *ddbbackend.DynamoDBHandler
	S3Ops     *s3backend.S3Handler
	SSMOps    *ssmbackend.Handler
	IAMOps    *iambackend.Handler
	STSOps    *stsbackend.Handler
	SNSOps    *snsbackend.Handler
	SQSOps    *sqsbackend.Handler
	// KMSOps provides access to the KMS backend.
	KMSOps *kmsbackend.Handler
	// SecretsManagerOps provides access to the Secrets Manager backend.
	SecretsManagerOps *secretsmanagerbackend.Handler
	// LambdaOps provides access to the Lambda backend.
	LambdaOps *lambdabackend.Handler
	// EventBridgeOps provides access to the EventBridge backend.
	EventBridgeOps *ebbackend.Handler
	// APIGatewayOps provides access to the API Gateway backend.
	APIGatewayOps *apigwbackend.Handler
	// CloudWatchLogsOps provides access to the CloudWatch Logs backend.
	CloudWatchLogsOps *cwlogsbackend.Handler
	// StepFunctionsOps provides access to the Step Functions backend.
	StepFunctionsOps *sfnbackend.Handler
	// CloudWatchOps provides access to the CloudWatch Metrics backend.
	CloudWatchOps *cwbackend.Handler
	// CloudFormationOps provides access to the CloudFormation backend.
	CloudFormationOps *cfnbackend.Handler
	// KinesisOps provides access to the Kinesis backend.
	KinesisOps *kinesisbackend.Handler
	// ElastiCacheOps provides access to the ElastiCache backend.
	ElastiCacheOps *elasticachebackend.Handler
	// Route53Ops provides access to the Route 53 backend.
	Route53Ops *route53backend.Handler
	// SESOps provides access to the SES backend.
	SESOps *sesbackend.Handler
	// SESv2Ops provides access to the SES v2 backend.
	SESv2Ops *sesv2backend.Handler
	// EC2Ops provides access to the EC2 backend.
	EC2Ops *ec2backend.Handler
	// ECROps provides access to the ECR backend.
	ECROps *ecrbackend.Handler
	// ECSOps provides access to the ECS backend.
	ECSOps *ecsbackend.Handler
	// EFSOps provides access to the EFS backend.
	EFSOps *efsbackend.Handler
	// IoTOps provides access to the IoT backend.
	IoTOps *iotbackend.Handler
	// FISOps provides access to the FIS backend.
	FISOps *fisbackend.Handler
	// GlueOps provides access to the Glue backend.
	GlueOps *gluebackend.Handler
	// IdentityStoreOps provides access to the Identity Store backend.
	IdentityStoreOps *identitystorebackend.Handler
	// OpenSearchOps provides access to the OpenSearch backend.
	OpenSearchOps *opensearchbackend.Handler
	// ACMOps provides access to the ACM backend.
	ACMOps *acmbackend.Handler
	// ACMPCAOps provides access to the ACM PCA backend.
	ACMPCAOps *acmpcabackend.Handler
	// RedshiftOps provides access to the Redshift backend.
	RedshiftOps *redshiftbackend.Handler
	// RDSOps provides access to the RDS backend.
	RDSOps *rdsbackend.Handler
	// DocDBOps provides access to the DocDB backend.
	DocDBOps     *docdbbackend.Handler
	AWSConfigOps *awsconfigbackend.Handler
	// S3ControlOps provides access to the S3 Control backend.
	S3ControlOps *s3controlbackend.Handler
	// ResourceGroupsOps provides access to the Resource Groups backend.
	ResourceGroupsOps *resourcegroupsbackend.Handler
	// ResourceGroupsTaggingOps provides access to the Resource Groups Tagging API backend.
	ResourceGroupsTaggingOps *taggingbackend.Handler
	// SWFOps provides access to the SWF backend.
	SWFOps *swfbackend.Handler
	// FirehoseOps provides access to the Firehose backend.
	FirehoseOps *firehosebackend.Handler
	// SchedulerOps provides access to the EventBridge Scheduler backend.
	SchedulerOps *schedulerbackend.Handler
	// Route53ResolverOps provides access to the Route53 Resolver backend.
	Route53ResolverOps *route53resolverbackend.Handler
	// TranscribeOps provides access to the Transcribe backend.
	TranscribeOps *transcribebackend.Handler
	// SupportOps provides access to the Support backend.
	SupportOps *supportbackend.Handler
	// CognitoIdentityOps provides access to the Cognito Identity backend.
	CognitoIdentityOps *cognitoidentitybackend.Handler
	// AppSyncOps provides access to the AppSync backend.
	AppSyncOps *appsyncbackend.Handler
	// CognitoIDPOps provides access to the Cognito IDP backend.
	CognitoIDPOps *cognitoidpbackend.Handler
	// IoTDataPlaneOps provides access to the IoT Data Plane backend.
	IoTDataPlaneOps *iotdataplanebackend.Handler
	// APIGatewayManagementAPIOps provides access to the API Gateway Management API backend.
	APIGatewayManagementAPIOps *apigwmgmtbackend.Handler
	// APIGatewayV2Ops provides access to the API Gateway V2 backend.
	APIGatewayV2Ops *apigwv2backend.Handler
	// AppConfigDataOps provides access to the AppConfigData backend.
	AppConfigDataOps *appconfigdatabackend.Handler
	// AmplifyOps provides access to the Amplify backend.
	AmplifyOps *amplifybackend.Handler
	// AthenaOps provides access to the Athena backend.
	AthenaOps *athenabackend.Handler
	// AutoscalingOps provides access to the Autoscaling backend.
	AutoscalingOps *autoscalingbackend.Handler
	// BackupOps provides access to the Backup backend.
	BackupOps *backupbackend.Handler
	// CloudTrailOps provides access to the CloudTrail backend.
	CloudTrailOps *cloudtrailbackend.Handler
	// AppConfigOps provides access to the AppConfig backend.
	AppConfigOps *appconfigbackend.Handler
	// ApplicationAutoscalingOps provides access to the Application Auto Scaling backend.
	ApplicationAutoscalingOps *applicationautoscalingbackend.Handler
	// BatchOps provides access to the Batch backend.
	BatchOps *batchbackend.Handler
	// BedrockOps provides access to the Bedrock backend.
	BedrockOps *bedrockbackend.Handler
	// BedrockRuntimeOps provides access to the Bedrock Runtime backend.
	BedrockRuntimeOps *bedrockruntimebackend.Handler
	// CeOps provides access to the Cost Explorer backend.
	CeOps *cebackend.Handler
	// CloudControlOps provides access to the CloudControl backend.
	CloudControlOps *cloudcontrolbackend.Handler
	// CloudFrontOps provides access to the CloudFront backend.
	CloudFrontOps *cloudfrontbackend.Handler
	// CodeArtifactOps provides access to the CodeArtifact backend.
	CodeArtifactOps *codeartifactbackend.Handler
	// CodeBuildOps provides access to the CodeBuild backend.
	CodeBuildOps *codebuildbackend.Handler
	// CodeCommitOps provides access to the CodeCommit backend.
	CodeCommitOps *codecommitbackend.Handler
	// CodePipelineOps provides access to the CodePipeline backend.
	CodePipelineOps *codepipelinebackend.Handler
	// CodeConnectionsOps provides access to the CodeConnections backend.
	CodeConnectionsOps *codeconnectionsbackend.Handler
	// CodeDeployOps provides access to the CodeDeploy backend.
	CodeDeployOps *codedeploybackend.Handler
	// DMSOps provides access to the DMS backend.
	DMSOps *dmsbackend.Handler
	// CodeStarConnectionsOps provides access to the CodeStar Connections backend.
	CodeStarConnectionsOps *codestarconnectionsbackend.Handler
	// DynamoDBStreamsOps provides access to the DynamoDB Streams backend.
	DynamoDBStreamsOps *dynamodbstreamsbackend.Handler
	// ElasticbeanstalkOps provides access to the Elastic Beanstalk backend.
	ElasticbeanstalkOps *elasticbeanstalkbackend.Handler
	// ElasticsearchOps provides access to the Elasticsearch backend.
	ElasticsearchOps *elasticsearchbackend.Handler
	// EKSOps provides access to the EKS backend.
	EKSOps *eksbackend.Handler
	// ElasticTranscoderOps provides access to the Elastic Transcoder backend.
	ElasticTranscoderOps *elastictranscoderbackend.Handler
	// ELBOps provides access to the Classic ELB backend.
	ELBOps *elbbackend.Handler
	// ELBv2Ops provides access to the ELBv2 (ALB/NLB) backend.
	ELBv2Ops *elbv2backend.Handler
	// EmrServerlessOps provides access to the EMR Serverless backend.
	EmrServerlessOps *emrserverlessbackend.Handler
	// EMROps provides access to the EMR backend.
	EMROps *emrbackend.Handler
	// GlacierOps provides access to the Glacier backend.
	GlacierOps *glacierbackend.Handler
	// IoTAnalyticsOps provides access to the IoT Analytics backend.
	IoTAnalyticsOps *iotanalyticsbackend.Handler
	// IoTWirelessOps provides access to the IoT Wireless backend.
	IoTWirelessOps *iotwirelessbackend.Handler
	// KinesisAnalyticsOps provides access to the Kinesis Analytics backend.
	KinesisAnalyticsOps *kinesisanalyticsbackend.Handler
	// KafkaOps provides access to the MSK Kafka backend.
	KafkaOps *kafkabackend.Handler
	// KinesisAnalyticsV2Ops provides access to the Kinesis Data Analytics v2 backend.
	KinesisAnalyticsV2Ops *kinesisanalyticsv2backend.Handler
	// LakeFormationOps provides access to the Lake Formation backend.
	LakeFormationOps *lakeformationbackend.Handler
	// ManagedBlockchainOps provides access to the Managed Blockchain backend.
	ManagedBlockchainOps *managedblockchainbackend.Handler
	// MediaConvertOps provides access to the MediaConvert backend.
	MediaConvertOps *mediaconvertbackend.Handler
	// MQOps provides access to the Amazon MQ backend.
	MQOps *mqbackend.Handler
	// MediaStoreOps provides access to the MediaStore backend.
	MediaStoreOps *mediastorebackend.Handler
	// MediaStoreDataOps provides access to the MediaStore Data backend.
	MediaStoreDataOps *mediastoredatabackend.Handler
	// MemoryDBOps provides access to the MemoryDB backend.
	MemoryDBOps *memorydbbackend.Handler
	// OrganizationsOps provides access to the Organizations backend.
	OrganizationsOps *organizationsbackend.Handler
	// MWAAOps provides access to the MWAA backend.
	MWAAOps *mwaabackend.Handler
	// PinpointOps provides access to the Pinpoint backend.
	PinpointOps *pinpointbackend.Handler
	// NeptuneOps provides access to the Neptune backend.
	NeptuneOps *neptunebackend.Handler
	// PipesOps provides access to the EventBridge Pipes backend.
	PipesOps *pipesbackend.Handler
	// QLDBOps provides access to the QLDB backend.
	QLDBOps *qldbbackend.Handler
	// QLDBSessionOps provides access to the QLDB Session backend.
	QLDBSessionOps *qldbsessionbackend.Handler
	// RDSDataOps provides access to the RDS Data backend.
	RDSDataOps *rdsdatabackend.Handler
	// RAMOps provides access to the RAM backend.
	RAMOps *rambackend.Handler
	// RedshiftDataOps provides access to the Redshift Data backend.
	RedshiftDataOps *redshiftdatabackend.Handler
	// SageMakerOps provides access to the SageMaker backend.
	SageMakerOps *sagemakerbackend.Handler
	// SageMakerRuntimeOps provides access to the SageMaker Runtime backend.
	SageMakerRuntimeOps *sagemakerruntimebackend.Handler
	// ServiceDiscoveryOps provides access to the Service Discovery backend.
	ServiceDiscoveryOps *servicediscoverybackend.Handler
	// ServerlessRepoOps provides access to the Serverless Application Repository backend.
	ServerlessRepoOps *serverlessrepobackend.Handler
	// ShieldOps provides access to the Shield backend.
	ShieldOps *shieldbackend.Handler
	// SsoAdminOps provides access to the SSO Admin backend.
	SsoAdminOps *ssoadminbackend.Handler
	// TextractOps provides access to the Textract backend.
	TextractOps *textractbackend.Handler
	// TimestreamWriteOps provides access to the Timestream Write backend.
	TimestreamWriteOps *timestreamwritebackend.Handler
	// TimestreamQueryOps provides access to the Timestream Query backend.
	TimestreamQueryOps *timestreamquerybackend.Handler
	// TransferOps provides access to the Transfer backend.
	TransferOps *transferbackend.Handler
	// VerifiedPermissionsOps provides access to the Verified Permissions backend.
	VerifiedPermissionsOps *verifiedpermissionsbackend.Handler
	// Wafv2Ops provides access to the WAFv2 backend.
	Wafv2Ops *wafv2backend.Handler
	// XrayOps provides access to the X-Ray backend.
	XrayOps *xraybackend.Handler
	// S3TablesOps provides access to the S3 Tables backend.
	S3TablesOps *s3tablesbackend.Handler
	// FaultStore provides access to the Chaos fault store for the dashboard UI.
	FaultStore *chaos.FaultStore
	// PortAlloc provides access to runtime-allocated service ports.
	PortAlloc *portalloc.Allocator
	// Logger is the structured logger for dashboard operations.
	Logger        *slog.Logger
	GlobalConfig  *config.GlobalConfig
	ConfigManager ConfigManager
}

// NewHandler creates a new Dashboard web handler.
func NewHandler(cfg Config) *DashboardHandler {
	return &DashboardHandler{
		FaultStore:    cfg.FaultStore,
		ConfigManager: cfg.ConfigManager,
		GlobalConfig:  cfg.GlobalConfig,
		Logger:        cfg.Logger,
		config:        cfg,
	}
}

// Name returns the service name.
func (h *DashboardHandler) Name() string {
	return "Dashboard"
}

// Handler returns the Echo handler function for this service.
func (h *DashboardHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		h.mu.RLock()
		sr := h.SubRouter
		h.mu.RUnlock()

		if sr == nil {
			h.mu.Lock()
			if h.SubRouter == nil {
				h.SubRouter = echo.New()
				h.setupSubRouter()
			}
			sr = h.SubRouter
			h.mu.Unlock()
		}

		sr.ServeHTTP(c.Response(), c.Request())

		return nil
	}
}

// setupSubRouter registers all routes for the dashboard.
//
//nolint:gocognit,gocyclo,cyclop,dupl,funlen // centralized route wiring keeps setup behavior explicit.
func (h *DashboardHandler) setupSubRouter() {
	// Static assets
	h.SubRouter.GET("/dashboard/static/*", func(c *echo.Context) error {
		http.StripPrefix("/dashboard", http.FileServer(http.FS(staticFS))).
			ServeHTTP(c.Response(), c.Request())

		return nil
	})

	// Legacy REST endpoints for test compatibility (Terraform)
	h.SubRouter.POST("/dashboard/apigatewaymanagementapi/connection/create", h.apiGatewayManagementAPICreateConnection)
	h.SubRouter.POST("/dashboard/appconfigdata/configuration/set", h.appConfigDataSetConfiguration)

	// Connect-RPC server
	connectPath, connectHandler := dashboardv1connect.NewDashboardServiceHandler(NewConnectDashboardServer())
	h.SubRouter.Any(connectPath+"*", echo.WrapHandler(connectHandler))

	h.SubRouter.GET("/dashboard/api/codestarconnections/connections", func(c *echo.Context) error {
		if h.config.CodeStarConnectionsOps == nil || h.config.CodeStarConnectionsOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"connections": []any{}})
		}

		return c.JSON(http.StatusOK, map[string]any{
			"connections": h.config.CodeStarConnectionsOps.Backend.ListConnections("", ""),
		})
	})

	h.SubRouter.GET("/dashboard/api/system/state", func(c *echo.Context) error {
		const (
			defaultPortRangeStart = 5000
			defaultPortRangeEnd   = 10000
		)

		type allocatedPort struct {
			Label string `json:"label"`
			Port  int    `json:"port"`
		}

		portRangeStart := defaultPortRangeStart
		portRangeEnd := defaultPortRangeEnd
		if h.ConfigManager != nil {
			settings := h.ConfigManager.GetSettings()
			if settings.PortRangeStart > 0 {
				portRangeStart = settings.PortRangeStart
			}
			if settings.PortRangeEnd > settings.PortRangeStart {
				portRangeEnd = settings.PortRangeEnd
			}
		}

		allocated := make([]allocatedPort, 0)
		if h.config.PortAlloc != nil {
			snapshot := h.config.PortAlloc.Allocated()
			allocated = make([]allocatedPort, 0, len(snapshot))
			for port, label := range snapshot {
				allocated = append(allocated, allocatedPort{Port: port, Label: label})
			}
			sort.Slice(allocated, func(i, j int) bool {
				return allocated[i].Port < allocated[j].Port
			})
		}

		return c.JSON(http.StatusOK, map[string]any{
			"portRangeStart": portRangeStart,
			"portRangeEnd":   portRangeEnd,
			"allocated":      allocated,
		})
	})

	h.SubRouter.POST("/dashboard/api/codestarconnections/connections", func(c *echo.Context) error {
		if h.config.CodeStarConnectionsOps == nil || h.config.CodeStarConnectionsOps.Backend == nil {
			return c.JSON(
				http.StatusServiceUnavailable,
				map[string]string{"error": "codestar connections backend unavailable"},
			)
		}

		var req struct {
			ConnectionName string `json:"connectionName"`
			ProviderType   string `json:"providerType"`
			HostArn        string `json:"hostArn"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		conn, err := h.config.CodeStarConnectionsOps.Backend.CreateConnection(
			req.ConnectionName,
			req.ProviderType,
			req.HostArn,
			nil,
		)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, conn)
	})

	h.SubRouter.GET("/dashboard/api/codestarconnections/hosts", func(c *echo.Context) error {
		if h.config.CodeStarConnectionsOps == nil || h.config.CodeStarConnectionsOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"hosts": []any{}})
		}

		return c.JSON(http.StatusOK, map[string]any{
			"hosts": h.config.CodeStarConnectionsOps.Backend.ListHosts(),
		})
	})

	h.SubRouter.POST("/dashboard/api/codestarconnections/hosts", func(c *echo.Context) error {
		if h.config.CodeStarConnectionsOps == nil || h.config.CodeStarConnectionsOps.Backend == nil {
			return c.JSON(
				http.StatusServiceUnavailable,
				map[string]string{"error": "codestar connections backend unavailable"},
			)
		}

		var req struct {
			Name             string `json:"name"`
			ProviderType     string `json:"providerType"`
			ProviderEndpoint string `json:"providerEndpoint"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		host, err := h.config.CodeStarConnectionsOps.Backend.CreateHost(
			req.Name,
			req.ProviderType,
			req.ProviderEndpoint,
			nil,
		)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, host)
	})

	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"userPools": []any{}})
		}

		return c.JSON(http.StatusOK, map[string]any{
			"userPools": h.config.CognitoIDPOps.Backend.ListUserPools(),
		})
	})

	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		pool, err := h.config.CognitoIDPOps.Backend.CreateUserPool(req.Name)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, pool)
	})

	h.SubRouter.DELETE("/dashboard/api/cognitoidp/user-pools/:id", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.DeleteUserPool(c.Param("id")); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// Clients
	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools/:id/clients", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"clients": []any{}})
		}

		clients, err := h.config.CognitoIDPOps.Backend.ListUserPoolClients(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, map[string]any{"clients": clients})
	})

	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools/:id/clients", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			ClientName string `json:"clientName"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		client, err := h.config.CognitoIDPOps.Backend.CreateUserPoolClient(c.Param("id"), req.ClientName)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, client)
	})

	h.SubRouter.DELETE("/dashboard/api/cognitoidp/user-pools/:id/clients/:clientId", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.DeleteUserPoolClient(c.Param("id"), c.Param("clientId")); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// Users
	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools/:id/users", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"users": []any{}})
		}

		users, err := h.config.CognitoIDPOps.Backend.ListUsers(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, map[string]any{"users": users})
	})

	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools/:id/users", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			Username     string `json:"username"`
			TempPassword string `json:"tempPassword"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		user, err := h.config.CognitoIDPOps.Backend.AdminCreateUser(c.Param("id"), req.Username, req.TempPassword, nil)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, user)
	})

	h.SubRouter.DELETE("/dashboard/api/cognitoidp/user-pools/:id/users/:username", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.AdminDeleteUser(c.Param("id"), c.Param("username")); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	h.SubRouter.PATCH("/dashboard/api/cognitoidp/user-pools/:id/users/:username", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		var opErr error
		if req.Enabled {
			opErr = h.config.CognitoIDPOps.Backend.AdminEnableUser(c.Param("id"), c.Param("username"))
		} else {
			opErr = h.config.CognitoIDPOps.Backend.AdminDisableUser(c.Param("id"), c.Param("username"))
		}
		if opErr != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": opErr.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// Groups
	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools/:id/groups", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"groups": []any{}})
		}

		groups, err := h.config.CognitoIDPOps.Backend.ListGroups(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, map[string]any{"groups": groups})
	})

	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools/:id/groups", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			GroupName   string `json:"groupName"`
			Description string `json:"description"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		group, err := h.config.CognitoIDPOps.Backend.CreateGroup(c.Param("id"), req.GroupName, req.Description, 0)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusCreated, group)
	})

	h.SubRouter.DELETE("/dashboard/api/cognitoidp/user-pools/:id/groups/:groupName", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.DeleteGroup(c.Param("id"), c.Param("groupName")); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// Pool update (MFA config)
	h.SubRouter.PATCH("/dashboard/api/cognitoidp/user-pools/:id", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		var req struct {
			MfaConfiguration string `json:"mfaConfiguration"`
		}
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		if err := h.config.CognitoIDPOps.Backend.UpdateUserPool(c.Param("id"), req.MfaConfiguration); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// Pool metrics
	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools/:id/metrics", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		metrics, err := h.config.CognitoIDPOps.Backend.GetPoolMetrics(c.Param("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, metrics)
	})

	// Group members: list, add, remove
	h.SubRouter.GET("/dashboard/api/cognitoidp/user-pools/:id/groups/:groupName/members", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"users": []any{}})
		}

		users, err := h.config.CognitoIDPOps.Backend.ListUsersInGroup(c.Param("id"), c.Param("groupName"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.JSON(http.StatusOK, map[string]any{"users": users})
	})

	h.SubRouter.PUT(
		"/dashboard/api/cognitoidp/user-pools/:id/groups/:groupName/members/:username",
		func(c *echo.Context) error {
			if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
				return c.JSON(
					http.StatusServiceUnavailable,
					map[string]string{"error": "cognito idp backend unavailable"},
				)
			}

			if err := h.config.CognitoIDPOps.Backend.AdminAddUserToGroup(
				c.Param("id"), c.Param("username"), c.Param("groupName"),
			); err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
			}

			return c.NoContent(http.StatusNoContent)
		},
	)

	h.SubRouter.DELETE(
		"/dashboard/api/cognitoidp/user-pools/:id/groups/:groupName/members/:username",
		func(c *echo.Context) error {
			if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
				return c.JSON(
					http.StatusServiceUnavailable,
					map[string]string{"error": "cognito idp backend unavailable"},
				)
			}

			if err := h.config.CognitoIDPOps.Backend.AdminRemoveUserFromGroup(
				c.Param("id"), c.Param("username"), c.Param("groupName"),
			); err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
			}

			return c.NoContent(http.StatusNoContent)
		},
	)

	// User admin reset password
	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools/:id/users/:username/reset", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.AdminResetUserPassword(
			c.Param("id"),
			c.Param("username"),
		); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	// User admin global sign-out
	h.SubRouter.POST("/dashboard/api/cognitoidp/user-pools/:id/users/:username/sign-out", func(c *echo.Context) error {
		if h.config.CognitoIDPOps == nil || h.config.CognitoIDPOps.Backend == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "cognito idp backend unavailable"})
		}

		if err := h.config.CognitoIDPOps.Backend.AdminUserGlobalSignOut(
			c.Param("id"),
			c.Param("username"),
		); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		return c.NoContent(http.StatusNoContent)
	})

	h.SubRouter.GET("/dashboard/api/mediastoredata/objects", func(c *echo.Context) error {
		if h.config.MediaStoreDataOps == nil || h.config.MediaStoreDataOps.Backend == nil {
			return c.JSON(http.StatusOK, map[string]any{"objects": []string{}})
		}

		items := h.config.MediaStoreDataOps.Backend.ListAllObjects()
		objectPaths := make([]string, 0, len(items))
		for _, item := range items {
			if item == nil || item.Name == "" {
				continue
			}

			objectPaths = append(objectPaths, strings.TrimPrefix(item.Name, "/"))
		}

		return c.JSON(http.StatusOK, map[string]any{"objects": objectPaths})
	})

	h.SubRouter.GET("/dashboard/dynamodb/table/:name/stream-events", func(c *echo.Context) error {
		name := c.Param("name")
		if h.config.DynamoDBStreamsOps == nil || h.config.DynamoDBStreamsOps.Streams == nil {
			return c.HTML(http.StatusOK, "Streams backend unavailable")
		}

		// Use the backend to get recent stream events and format as minimal HTML
		events := h.config.DynamoDBStreamsOps.Streams.GetRecentEvents(name)
		if len(events) == 0 {
			return c.HTML(http.StatusOK, "<p class='text-slate-400 text-xs italic'>No recent stream events.</p>")
		}

		var sb strings.Builder
		sb.WriteString("<table class='w-full text-xs font-mono border-collapse'>")
		sb.WriteString(
			"<thead><tr class='border-b border-slate-200 dark:border-slate-700'>" +
				"<th class='text-left pb-1 pr-3 text-slate-500 font-medium'>Event</th>" +
				"<th class='text-left pb-1 pr-3 text-slate-500 font-medium'>Sequence</th>" +
				"<th class='text-left pb-1 text-slate-500 font-medium'>Time</th>" +
				"</tr></thead>",
		)
		const seqSuffixLen = 12
		sb.WriteString("<tbody>")
		// Show most recent first
		for i := len(events) - 1; i >= 0; i-- {
			e := events[i]
			var eventClass string
			switch e.EventName {
			case "INSERT":
				eventClass = "text-green-700 dark:text-green-400"
			case "REMOVE":
				eventClass = "text-red-700 dark:text-red-400"
			default:
				eventClass = "text-blue-700 dark:text-blue-400"
			}
			ts := time.Unix(e.ApproximateCreationDateTime, 0).Format("15:04:05")
			seq := e.SequenceNumber
			if len(seq) > seqSuffixLen {
				seq = seq[len(seq)-seqSuffixLen:]
			}
			sb.WriteString(
				"<tr class='border-b border-slate-100 dark:border-slate-700 " +
					"hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors'>",
			)
			sb.WriteString(
				"<td class='py-1 pr-3 font-semibold " + eventClass + "'>" + html.EscapeString(e.EventName) + "</td>",
			)
			sb.WriteString(
				"<td class='py-1 pr-3 text-slate-500 dark:text-slate-400'>…" + html.EscapeString(seq) + "</td>",
			)
			sb.WriteString("<td class='py-1 text-slate-500 dark:text-slate-400'>" + html.EscapeString(ts) + "</td>")
			sb.WriteString("</tr>")
		}
		sb.WriteString("</tbody></table>")

		return c.HTML(http.StatusOK, sb.String())
	})

	// SPA fallback
	h.SubRouter.Any("/dashboard/*", h.spaFallbackHandler())
	h.SubRouter.Any("/dashboard", func(c *echo.Context) error {
		return c.Redirect(http.StatusMovedPermanently, "/dashboard/")
	})
}

// spaFallbackHandler serves the Svelte SPA.
func (h *DashboardHandler) spaFallbackHandler() echo.HandlerFunc {
	spaSubFS, err := fs.Sub(spaFS, "static/spa")
	if err != nil {
		panic("could not create subdirectory fs for static/spa: " + err.Error())
	}

	return func(c *echo.Context) error {
		reqPath := c.Request().URL.Path

		// Let Connect-RPC and static routes pass through if they reached here
		if strings.HasPrefix(reqPath, "/dashboard/static/") || strings.HasPrefix(reqPath, "/dashboard/api/") {
			return echo.ErrNotFound
		}

		// Try to serve a matching SPA asset first
		if served := trySPAAsset(c, spaSubFS, reqPath); served {
			return nil
		}

		// Fallback to index.html for client-side routing
		http.ServeFileFS(c.Response(), c.Request(), spaSubFS, "index.html")

		return nil
	}
}

// trySPAAsset attempts to serve a matching SPA static asset.
func trySPAAsset(c *echo.Context, spaFS fs.FS, reqPath string) bool {
	cleanPath := strings.TrimPrefix(reqPath, "/dashboard")
	if cleanPath == "" || cleanPath == "/" {
		return false
	}

	relPath := strings.TrimPrefix(path.Clean(cleanPath), "/")

	file, openErr := spaFS.Open(relPath)
	if openErr != nil {
		return false
	}

	stat, statErr := file.Stat()
	_ = file.Close()

	if statErr != nil || stat.IsDir() {
		return false
	}

	http.ServeFileFS(c.Response(), c.Request(), spaFS, relPath)

	return true
}

// RouteMatcher returns a function that determines if the request should be routed to this service.
func (h *DashboardHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		reqPath := c.Request().URL.Path
		method := c.Request().Method

		// ConnectRPC endpoints (server-streaming RPCs) use POST.
		if strings.HasPrefix(reqPath, "/"+dashboardv1connect.DashboardServiceName+"/") {
			return method == http.MethodPost
		}

		// Only match /dashboard and /dashboard2.
		// Important: Keep it non-greedy to avoid intercepting /_gopherstack/health.
		return strings.HasPrefix(reqPath, "/dashboard") || strings.HasPrefix(reqPath, "/dashboard2")
	}
}

// MatchPriority returns the priority for this service's matcher.
func (h *DashboardHandler) MatchPriority() int {
	return service.PriorityPathUI
}

// ExtractOperation extracts the operation name from the request.
func (h *DashboardHandler) ExtractOperation(_ *echo.Context) string {
	return ""
}

// ExtractResource extracts the resource identifier from the request.
func (h *DashboardHandler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns a list of operations supported by the dashboard (none).
func (h *DashboardHandler) GetSupportedOperations() []string {
	return nil
}

// apiGatewayManagementAPICreateConnection handles POST /dashboard/apigatewaymanagementapi/connection/create.
// This is a legacy endpoint used by terraform tests to seed data.
func (h *DashboardHandler) apiGatewayManagementAPICreateConnection(c *echo.Context) error {
	if h.config.APIGatewayManagementAPIOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	connectionID := c.Request().FormValue("connectionId")
	sourceIP := c.Request().FormValue("sourceIp")
	userAgent := c.Request().FormValue("userAgent")

	if connectionID == "" {
		return c.String(http.StatusBadRequest, "connectionId is required")
	}

	if sourceIP == "" {
		sourceIP = "127.0.0.1"
	}

	if userAgent == "" {
		userAgent = "test-client/1.0"
	}

	if _, err := h.config.APIGatewayManagementAPIOps.Backend.CreateConnection(
		connectionID,
		sourceIP,
		userAgent,
	); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/apigatewaymanagementapi")
}

// appConfigDataSetConfiguration handles POST /dashboard/appconfigdata/configuration/set.
// This is a legacy endpoint used by terraform tests to seed data.
func (h *DashboardHandler) appConfigDataSetConfiguration(c *echo.Context) error {
	if h.config.AppConfigDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	app := c.Request().FormValue("applicationIdentifier")
	env := c.Request().FormValue("environmentIdentifier")
	profile := c.Request().FormValue("configurationProfileIdentifier")
	content := c.Request().FormValue("content")
	contentType := c.Request().FormValue("contentType")

	if app == "" || env == "" || profile == "" {
		return c.String(
			http.StatusBadRequest,
			"applicationIdentifier, environmentIdentifier, and configurationProfileIdentifier are required",
		)
	}

	if contentType == "" {
		contentType = "application/json"
	}

	h.config.AppConfigDataOps.Backend.SetConfiguration(app, env, profile, content, contentType)

	return c.Redirect(http.StatusFound, "/dashboard/appconfigdata")
}
