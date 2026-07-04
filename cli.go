package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	appsyncsdksvc "github.com/aws/aws-sdk-go-v2/service/appsync"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	codepipelinesdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsddbstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	ddbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	snsevents "github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/version"
	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigwmgmtbackend "github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	appconfigdatabackend "github.com/blackbirdworks/gopherstack/services/appconfigdata"
	applicationautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	appmeshbackend "github.com/blackbirdworks/gopherstack/services/appmesh"
	apprunnerbackend "github.com/blackbirdworks/gopherstack/services/apprunner"
	appstreambackend "github.com/blackbirdworks/gopherstack/services/appstream"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	bedrockbackend "github.com/blackbirdworks/gopherstack/services/bedrock"
	bedrockagentbackend "github.com/blackbirdworks/gopherstack/services/bedrockagent"
	bedrockruntimebackend "github.com/blackbirdworks/gopherstack/services/bedrockruntime"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	cleanroomsbackend "github.com/blackbirdworks/gopherstack/services/cleanrooms"
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
	comprehendbackend "github.com/blackbirdworks/gopherstack/services/comprehend"
	databrewbackend "github.com/blackbirdworks/gopherstack/services/databrew"
	datasyncbackend "github.com/blackbirdworks/gopherstack/services/datasync"
	daxbackend "github.com/blackbirdworks/gopherstack/services/dax"
	detectivebackend "github.com/blackbirdworks/gopherstack/services/detective"
	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
	dlmbackend "github.com/blackbirdworks/gopherstack/services/dlm"
	dmsbackend "github.com/blackbirdworks/gopherstack/services/dms"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ddbmodels "github.com/blackbirdworks/gopherstack/services/dynamodb/models"
	dynamodbstreamsbackend "github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	elasticbeanstalkbackend "github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	emrserverlessbackend "github.com/blackbirdworks/gopherstack/services/emrserverless"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	forecastbackend "github.com/blackbirdworks/gopherstack/services/forecast"
	fsxbackend "github.com/blackbirdworks/gopherstack/services/fsx"
	glacierbackend "github.com/blackbirdworks/gopherstack/services/glacier"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	guarddutybackend "github.com/blackbirdworks/gopherstack/services/guardduty"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
	inspector2backend "github.com/blackbirdworks/gopherstack/services/inspector2"
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
	macie2backend "github.com/blackbirdworks/gopherstack/services/macie2"
	managedblockchainbackend "github.com/blackbirdworks/gopherstack/services/managedblockchain"
	mediaconvertbackend "github.com/blackbirdworks/gopherstack/services/mediaconvert"
	medialivebackend "github.com/blackbirdworks/gopherstack/services/medialive"
	mediapackagebackend "github.com/blackbirdworks/gopherstack/services/mediapackage"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	mediastoredatabackend "github.com/blackbirdworks/gopherstack/services/mediastoredata"
	mediatailorbackend "github.com/blackbirdworks/gopherstack/services/mediatailor"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	mwaabackend "github.com/blackbirdworks/gopherstack/services/mwaa"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	networkmonitorbackend "github.com/blackbirdworks/gopherstack/services/networkmonitor"
	omicsbackend "github.com/blackbirdworks/gopherstack/services/omics"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	personalizebackend "github.com/blackbirdworks/gopherstack/services/personalize"
	pinpointbackend "github.com/blackbirdworks/gopherstack/services/pinpoint"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	pollybackend "github.com/blackbirdworks/gopherstack/services/polly"
	quicksightbackend "github.com/blackbirdworks/gopherstack/services/quicksight"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	rdsdatabackend "github.com/blackbirdworks/gopherstack/services/rdsdata"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	rekognitionbackend "github.com/blackbirdworks/gopherstack/services/rekognition"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	resourcegroupstaggingapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	rolesanywherebackend "github.com/blackbirdworks/gopherstack/services/rolesanywhere"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
	s3tablesbackend "github.com/blackbirdworks/gopherstack/services/s3tables"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	sagemakerruntimebackend "github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	securityhubbackend "github.com/blackbirdworks/gopherstack/services/securityhub"
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
	translatebackend "github.com/blackbirdworks/gopherstack/services/translate"
	verifiedpermissionsbackend "github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
	vpclatticebackend "github.com/blackbirdworks/gopherstack/services/vpclattice"
	wafbackend "github.com/blackbirdworks/gopherstack/services/waf"
	wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"
	workmailbackend "github.com/blackbirdworks/gopherstack/services/workmail"
	workspacesbackend "github.com/blackbirdworks/gopherstack/services/workspaces"
	xraybackend "github.com/blackbirdworks/gopherstack/services/xray"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const (
	defaultPort              = "8000"
	defaultRegion            = "us-east-1"
	defaultTimeout           = 30 * time.Second
	shutdownTimeout          = 5 * time.Second
	healthCheckTimeout       = 5 * time.Second
	configFilename           = "config.json"
	defaultReadHeaderTimeout = 5 * time.Second
	configDirPerm            = 0o700
	configFilePerm           = 0o600

	// selfSignedValidity is how long a generated self-signed TLS cert is valid.
	selfSignedValidity = 365 * 24 * time.Hour
	// selfSignedSerialBits is the bit-length of the random certificate serial.
	selfSignedSerialBits = 128
	// localhostName is the hostname the self-signed dev certificate is issued for.
	localhostName = "localhost"
	// loopbackIPv4Octet is the first octet of the IPv4 loopback address (127.x).
	loopbackIPv4Octet = 127

	keyMessageField      = "message"
	logLevelDebug        = "debug"
	demoAppName          = "demo-app"
	contentTypeJSON      = "application/json"
	emrServerlessRoleARN = "arn:aws:iam::000000000000:role/EMRServerlessRole"
	envProduction        = "production"
	kinesisServiceName   = "kinesis"
)

// CLI holds all command-line / environment-variable configuration for Gopherstack.
type CLI struct {
	SecretsManager                struct{}            `embed:"" prefix:"secretsmanager-"`
	SQS                           sqsbackend.Settings `embed:"" prefix:"sqs-"`
	SNS                           struct{}            `embed:"" prefix:"sns-"`
	IAM                           struct{}            `embed:"" prefix:"iam-"`
	kinesisHandler                service.Registerable
	athenaHandler                 service.Registerable
	emrserverlessHandler          service.Registerable
	s3tablesHandler               service.Registerable
	xrayHandler                   service.Registerable
	wafHandler                    service.Registerable
	wafv2Handler                  service.Registerable
	verifiedPermissionsHandler    service.Registerable
	transferHandler               service.Registerable
	timestreamqueryHandler        service.Registerable
	timestreamwriteHandler        service.Registerable
	textractHandler               service.Registerable
	ssoadminHandler               service.Registerable
	shieldHandler                 service.Registerable
	serverlessrepoHandler         service.Registerable
	elasticacheHandler            service.Registerable
	iotHandler                    service.Registerable
	ddbHandler                    service.Registerable
	s3Handler                     service.Registerable
	ssmHandler                    service.Registerable
	iamHandler                    service.Registerable
	stsHandler                    service.Registerable
	snsHandler                    service.Registerable
	sqsHandler                    service.Registerable
	lambdaHandler                 service.Registerable
	eventBridgeHandler            service.Registerable
	apiGatewayHandler             service.Registerable
	cognitoIDPHandler             service.Registerable
	stepFunctionsHandler          service.Registerable
	cloudWatchHandler             service.Registerable
	cloudFormationHandler         service.Registerable
	kmsHandler                    service.Registerable
	route53Handler                service.Registerable
	sesHandler                    service.Registerable
	sesv2Handler                  service.Registerable
	ec2Handler                    service.Registerable
	elasticsearchHandler          service.Registerable
	openSearchHandler             service.Registerable
	acmHandler                    service.Registerable
	acmpcaHandler                 service.Registerable
	redshiftHandler               service.Registerable
	redshiftServerlessHandler     service.Registerable
	rdsHandler                    service.Registerable
	awsconfigHandler              service.Registerable
	sagemakerRuntimeHandler       service.Registerable
	resourcegroupsHandler         service.Registerable
	resourcegroupstaggingHandler  service.Registerable
	swfHandler                    service.Registerable
	firehoseHandler               service.Registerable
	networkmonitorHandler         service.Registerable
	schedulerHandler              service.Registerable
	servicediscoveryHandler       service.Registerable
	transcribeHandler             service.Registerable
	supportHandler                service.Registerable
	appSyncHandler                service.Registerable
	iotDataPlaneHandler           service.Registerable
	apiGatewayMgmtHandler         service.Registerable
	appConfigDataHandler          service.Registerable
	amplifyHandler                service.Registerable
	autoscalingHandler            service.Registerable
	apiGatewayV2Handler           service.Registerable
	secretsManagerHandler         service.Registerable
	backupHandler                 service.Registerable
	cloudtrailHandler             service.Registerable
	appConfigHandler              service.Registerable
	applicationautoscalingHandler service.Registerable
	batchHandler                  service.Registerable
	bedrockHandler                service.Registerable
	bedrockAgentsHandler          service.Registerable
	bedrockruntimeHandler         service.Registerable
	ceHandler                     service.Registerable
	cloudcontrolHandler           service.Registerable
	cloudFrontHandler             service.Registerable
	codeArtifactHandler           service.Registerable
	codebuildHandler              service.Registerable
	codeCommitHandler             service.Registerable
	codePipelineHandler           service.Registerable
	codeConnectionsHandler        service.Registerable
	codeDeployHandler             service.Registerable
	comprehendHandler             service.Registerable
	dmsHandler                    service.Registerable
	codeStarConnectionsHandler    service.Registerable
	dynamodbStreamsHandler        service.Registerable
	docdbHandler                  service.Registerable
	elasticbeanstalkHandler       service.Registerable
	ecrHandler                    service.Registerable
	ecsHandler                    service.Registerable
	efsHandler                    service.Registerable
	eksHandler                    service.Registerable
	elbHandler                    service.Registerable
	elbv2Handler                  service.Registerable
	route53resolverHandler        service.Registerable
	cloudWatchLogsHandler         service.Registerable
	s3controlHandler              service.Registerable
	cognitoIdentityHandler        service.Registerable
	fisHandler                    service.Registerable
	identitystoreHandler          service.Registerable
	emrHandler                    service.Registerable
	glacierHandler                service.Registerable
	iotwirelessHandler            service.Registerable
	kinesisanalyticsHandler       service.Registerable
	lakeformationHandler          service.Registerable
	glueHandler                   service.Registerable
	guarddutyHandler              service.Registerable
	inspector2Handler             service.Registerable
	iotanalyticsHandler           service.Registerable
	kafkaHandler                  service.Registerable
	kinesisanalyticsv2Handler     service.Registerable
	managedblockchainHandler      service.Registerable
	mediaconvertHandler           service.Registerable
	mqHandler                     service.Registerable
	mediastoreHandler             service.Registerable
	mediastoredataHandler         service.Registerable
	memorydbHandler               service.Registerable
	organizationsHandler          service.Registerable
	mwaaHandler                   service.Registerable
	neptuneHandler                service.Registerable
	pinpointHandler               service.Registerable
	pipesHandler                  service.Registerable
	rdsdataHandler                service.Registerable
	ramHandler                    service.Registerable
	redshiftdataHandler           service.Registerable
	sagemakerHandler              service.Registerable
	ssmClient                     *ssmsdk.Client
	ecsClient                     *ecs.Client
	amplifyClient                 *amplifysdk.Client
	codePipelineSDKClient         *codepipelinesdk.Client
	codeDeployClient              *codedeploysdk.Client
	iotClient                     *iotsdk.Client
	eksClient                     *ekssdk.Client
	appSyncSdkClient              *appsyncsdksvc.Client
	ecrClient                     *ecr.Client
	secretsManagerClient          *secretsmanager.Client
	sqsClient                     *sqssdk.Client
	stsClient                     *stssdk.Client
	ddbClient                     *dynamodb.Client
	faultStore                    *chaos.FaultStore
	snsClient                     *sns.Client
	kmsClient                     *kms.Client
	iamClient                     *iam.Client
	s3Client                      *s3.Client
	globalConfig                  *config.GlobalConfig
	portAlloc                     *portalloc.Allocator
	ElastiCacheEngine             string                    `                                  name:"elasticache-engine"      env:"ELASTICACHE_ENGINE"      default:"embedded"      help:"ElastiCache engine mode: embedded (miniredis), stub, or docker."`                                      //nolint:lll // config struct tags are intentionally verbose
	EC2Provider                   string                    `                                  name:"ec2-provider"            env:"EC2_PROVIDER"            default:"inmemory"      help:"EC2 compute provider: inmemory (stub) or docker (launches real containers as instances)."`             //nolint:lll // config struct tags are intentionally verbose
	EC2DockerImage                string                    `                                  name:"ec2-docker-image"        env:"EC2_DOCKER_IMAGE"        default:"amazonlinux:2" help:"Docker image used by the EC2 docker provider when launching instances."`                               //nolint:lll // config struct tags are intentionally verbose
	EC2DockerNetwork              string                    `                                  name:"ec2-docker-network"      env:"EC2_DOCKER_NETWORK"      default:""              help:"Docker network EC2 docker-provider containers attach to (empty = daemon default bridge)."`             //nolint:lll // config struct tags are intentionally verbose
	EC2DockerSSHHostIP            string                    `                                  name:"ec2-docker-ssh-host-ip"  env:"EC2_DOCKER_SSH_HOST_IP"  default:"127.0.0.1"     help:"Host IP that mapped EC2-docker SSH ports bind to (use 0.0.0.0 to expose externally)."`                 //nolint:lll // config struct tags are intentionally verbose
	Port                          string                    `                                  name:"port"                    env:"PORT"                    default:"8000"          help:"HTTP server port."`                                                                                    //nolint:lll // config struct tags are intentionally verbose
	DataDir                       string                    `                                  name:"data-dir"                env:"GOPHERSTACK_DATA_DIR"    default:""              help:"Directory for persistence data files (default: ~/.gopherstack/data, or /data in containers)."`         //nolint:lll // config struct tags are intentionally verbose
	DNSListenAddr                 string                    `                                  name:"dns-addr"                env:"DNS_ADDR"                default:""              help:"Address for embedded DNS server (e.g. :10053). Empty = disabled."`                                     //nolint:lll // config struct tags are intentionally verbose
	LogLevel                      string                    `                                  name:"log-level"               env:"LOG_LEVEL"               default:"info"          help:"Log level (debug|info|warn|error)."`                                                                   //nolint:lll // config struct tags are intentionally verbose
	Region                        string                    `                                  name:"region"                  env:"REGION"                  default:"us-east-1"     help:"AWS region (also read from AWS_DEFAULT_REGION and AWS_REGION)."`                                       //nolint:lll // config struct tags are intentionally verbose
	OpenSearchEngine              string                    `                                  name:"opensearch-engine"       env:"OPENSEARCH_ENGINE"       default:"stub"          help:"OpenSearch engine mode: stub (API-only) or docker."`                                                   //nolint:lll // config struct tags are intentionally verbose
	ElasticsearchEngine           string                    `                                  name:"elasticsearch-engine"    env:"ELASTICSEARCH_ENGINE"    default:"stub"          help:"Elasticsearch engine mode: stub (API-only) or docker."`                                                //nolint:lll // config struct tags are intentionally verbose
	DNSResolveIP                  string                    `                                  name:"dns-resolve-ip"          env:"DNS_RESOLVE_IP"          default:"127.0.0.1"     help:"IP address synthetic hostnames resolve to."`                                                           //nolint:lll // config struct tags are intentionally verbose
	AccountID                     string                    `                                  name:"account-id"              env:"ACCOUNT_ID"              default:"000000000000"  help:"Mock AWS account ID used in ARNs."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	TLSCertFile                   string                    `                                  name:"tls-cert"                env:"TLS_CERT"                default:""              help:"Path to a TLS certificate (PEM). Enables an HTTPS listener; requires --tls-key. Empty = HTTP only."`   //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile                    string                    `                                  name:"tls-key"                 env:"TLS_KEY"                 default:""              help:"Path to a TLS private key (PEM). Required with --tls-cert."`                                           //nolint:lll // config struct tags are intentionally verbose
	SigV4Secret                   string                    `                                  name:"sigv4-secret"            env:"SIGV4_SECRET"            default:"test"          help:"Secret access key SigV4 validation signs against (used only when --validate-sigv4 is set)."`           //nolint:lll // config struct tags are intentionally verbose
	InitScripts                   []string                  `                                  name:"init-script"             env:"INIT_SCRIPTS"                                    help:"Shell scripts to run on startup (may be specified multiple times)."`                                   //nolint:lll // config struct tags are intentionally verbose
	S3InitBuckets                 []string                  `                                  name:"s3-bucket"               env:"S3_BUCKETS"                                      help:"S3 bucket names to create on startup (may be specified multiple times or as a comma-separated list)."` //nolint:lll // config struct tags are intentionally verbose
	S3                            s3backend.Settings        `embed:"" prefix:"s3-"`
	Lambda                        lambdabackend.Settings    `embed:"" prefix:"lambda-"`
	DynamoDB                      ddbbackend.Settings       `embed:"" prefix:"dynamodb-"`
	EC2                           ec2backend.Settings       `embed:"" prefix:"ec2-"`
	Batch                         batchbackend.Settings     `embed:"" prefix:"batch-"`
	CodeBuild                     codebuildbackend.Settings `embed:"" prefix:"codebuild-"`
	Backup                        backupbackend.Settings    `embed:"" prefix:"backup-"`
	SSM                           ssmbackend.Settings       `embed:"" prefix:"ssm-"`
	XRay                          xraybackend.Settings      `embed:"" prefix:"xray-"`
	SES                           sesbackend.Settings       `embed:"" prefix:"ses-"`
	FIS                           fisbackend.Settings       `embed:"" prefix:"fis-"`
	EMR                           emrbackend.Settings       `embed:"" prefix:"emr-"`
	Athena                        athenabackend.Settings    `embed:"" prefix:"athena-"`
	KMS                           kmsbackend.Settings       `embed:"" prefix:"kms-"`
	CloudWatchLogs                cwlogsbackend.Settings    `embed:"" prefix:"cloudwatchlogs-"`
	Kinesis                       kinesisbackend.Settings   `embed:"" prefix:"kinesis-"`
	STS                           stsbackend.Settings       `embed:"" prefix:"sts-"`
	StepFunctions                 sfnbackend.Settings       `embed:"" prefix:"stepfunctions-"`
	PortRangeStart                int                       `                                  name:"port-range-start"        env:"PORT_RANGE_START"        default:"10000"         help:"Start of the port range for resource endpoints."`                                                                                                                                              //nolint:lll // config struct tags are intentionally verbose
	PortRangeEnd                  int                       `                                  name:"port-range-end"          env:"PORT_RANGE_END"          default:"10100"         help:"End (exclusive) of the port range for resource endpoints."`                                                                                                                                    //nolint:lll // config struct tags are intentionally verbose
	EC2DockerSSHPortMin           int                       `                                  name:"ec2-docker-ssh-port-min" env:"EC2_DOCKER_SSH_PORT_MIN" default:"0"             help:"Lower bound of the host TCP port range used to map EC2-docker SSH (0 = let Docker pick)."`                                                                                                     //nolint:lll // config struct tags are intentionally verbose
	EC2DockerSSHPortMax           int                       `                                  name:"ec2-docker-ssh-port-max" env:"EC2_DOCKER_SSH_PORT_MAX" default:"0"             help:"Upper bound of the host TCP port range used to map EC2-docker SSH."`                                                                                                                           //nolint:lll // config struct tags are intentionally verbose
	InitScriptTimeout             time.Duration             `                                  name:"init-timeout"            env:"INIT_TIMEOUT"            default:"30s"           help:"Per-script timeout for init hooks."`                                                                                                                                                           //nolint:lll // config struct tags are intentionally verbose
	JanitorTimeout                time.Duration             `                                  name:"janitor-timeout"         env:"JANITOR_TIMEOUT"         default:"30s"           help:"Per-task timeout for janitor operations (TTL sweeps, table cleaners, etc.). Zero disables per-task timeouts. Higher values prevent deadlocks; lower values keep the janitor loop responsive."` //nolint:lll // config struct tags are intentionally verbose
	LatencyMs                     int                       `                                  name:"latency-ms"              env:"LATENCY_MS"              default:"0"             help:"Inject random latency [0,N) ms per request (0 = disabled). Values near the 30 s write timeout may cause connection errors."`                                                                   //nolint:lll // config struct tags are intentionally verbose
	AutoPurgeTTL                  time.Duration             `                                  name:"auto-purge-ttl"          env:"AUTO_PURGE_TTL"                                  help:"If set, automatically reset all services on a timer based on the TTL (e.g., 10m)."`                                                                                                            //nolint:lll // config struct tags are intentionally verbose
	EnforceIAM                    bool                      `                                  name:"enforce-iam"             env:"GOPHERSTACK_ENFORCE_IAM" default:"false"         help:"Enable IAM policy enforcement. When true, every AWS API request is evaluated against attached IAM policies."`                                                                                  //nolint:lll // config struct tags are intentionally verbose
	Persist                       bool                      `                                  name:"persist"                 env:"PERSIST"                 default:"false"         help:"Enable snapshot-based persistence across restarts."`                                                                                                                                           //nolint:lll // config struct tags are intentionally verbose
	Demo                          bool                      `                                  name:"demo"                    env:"DEMO"                    default:"false"         help:"Load demo data on startup."`                                                                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	TLS                           bool                      `                                  name:"tls"                     env:"TLS"                     default:"false"         help:"Serve over HTTPS. With --tls-cert/--tls-key uses those files; otherwise a self-signed certificate is generated on demand."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	ValidateSigV4                 bool                      `                                  name:"validate-sigv4"          env:"VALIDATE_SIGV4"          default:"false"         help:"Cryptographically validate AWS SigV4 request signatures (opt-in). Signed requests whose signature does not match --sigv4-secret are rejected."`                                                //nolint:lll // config struct tags are intentionally verbose
}

// GetGlobalConfig returns the centralised account ID and region (config.Provider).
func (c *CLI) GetGlobalConfig() *config.GlobalConfig {
	if c.globalConfig == nil {
		c.globalConfig = config.NewGlobalConfig(
			c.AccountID,
			c.Region,
			c.LatencyMs,
			c.JanitorTimeout,
			c.EnforceIAM,
			c.AutoPurgeTTL,
		)
	}

	return c.globalConfig
}

// resolvedDataDir returns the effective data directory for persistence.
func (c *CLI) resolvedDataDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}

	if _, err := os.Stat("/.dockerenv"); err == nil {
		return "/data"
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".gopherstack", "data")
	}

	return filepath.Join(home, ".gopherstack", "data")
}

// createPersistenceStore creates a FileStore using the resolved data directory.
func (c *CLI) createPersistenceStore() (*persistence.FileStore, error) {
	return persistence.NewFileStore(c.resolvedDataDir())
}

func (c *CLI) GetDynamoDBSettings() ddbbackend.Settings {
	return c.DynamoDB
}

// GetS3Settings returns S3 settings (s3.ConfigProvider).
func (c *CLI) GetS3Settings() s3backend.Settings {
	return c.S3
}

// GetS3Endpoint returns the configured S3 endpoint (s3.ConfigProvider).
func (c *CLI) GetS3Endpoint() string {
	s3Port := strings.TrimPrefix(c.Port, ":")

	return "localhost:" + s3Port
}

// GetEndpoint returns the base HTTP endpoint URL for this Gopherstack instance.
func (c *CLI) GetEndpoint() string {
	port := strings.TrimPrefix(c.Port, ":")

	return "http://localhost:" + port
}

// GetLambdaSettings returns Lambda settings (lambda.SettingsProvider).
func (c *CLI) GetLambdaSettings() lambdabackend.Settings {
	return c.Lambda
}

// GetStepFunctionsSettings returns Step Functions settings (stepfunctions.SettingsProvider).
func (c *CLI) GetStepFunctionsSettings() sfnbackend.Settings {
	return c.StepFunctions
}

// GetSSMSettings returns SSM settings (ssm.ConfigProvider).
func (c *CLI) GetSSMSettings() ssmbackend.Settings {
	return c.SSM
}

// GetKMSSettings returns KMS settings (kms.ConfigProvider).
func (c *CLI) GetKMSSettings() kmsbackend.Settings {
	return c.KMS
}

// GetSTSSettings returns STS settings (sts.ConfigProvider).
func (c *CLI) GetSTSSettings() stsbackend.Settings {
	return c.STS
}

// PersistableConfig holds settings that can be saved to disk.
type PersistableConfig struct {
	ElasticsearchEngine string                    `json:"elasticsearch_engine"`
	ElastiCacheEngine   string                    `json:"elasticache_engine"`
	Region              string                    `json:"region"`
	LogLevel            string                    `json:"log_level"`
	Port                string                    `json:"port"`
	OpenSearchEngine    string                    `json:"opensearch_engine"`
	AccountID           string                    `json:"account_id"`
	DataDir             string                    `json:"data_dir"`
	DNSListenAddr       string                    `json:"dns_listen_addr"`
	DNSResolveIP        string                    `json:"dns_resolve_ip"`
	S3                  s3backend.Settings        `json:"s3"`
	Lambda              lambdabackend.Settings    `json:"lambda"`
	DynamoDB            ddbbackend.Settings       `json:"dynamodb"`
	Batch               batchbackend.Settings     `json:"batch"`
	EC2                 ec2backend.Settings       `json:"ec2"`
	CodeBuild           codebuildbackend.Settings `json:"codebuild"`
	FIS                 fisbackend.Settings       `json:"fis"`
	Athena              athenabackend.Settings    `json:"athena"`
	EMR                 emrbackend.Settings       `json:"emr"`
	SES                 sesbackend.Settings       `json:"ses"`
	SSM                 ssmbackend.Settings       `json:"ssm"`
	XRay                xraybackend.Settings      `json:"xray"`
	Backup              backupbackend.Settings    `json:"backup"`
	PortRangeEnd        int                       `json:"port_range_end"`
	InitScriptTimeout   time.Duration             `json:"init_script_timeout"`
	LatencyMs           int                       `json:"latency_ms"`
	PortRangeStart      int                       `json:"port_range_start"`
	JanitorTimeout      time.Duration             `json:"janitor_timeout"`
	CloudWatchLogs      cwlogsbackend.Settings    `json:"cloudwatchlogs"`
	STS                 stsbackend.Settings       `json:"sts"`
	StepFunctions       sfnbackend.Settings       `json:"stepfunctions"`
	AutoPurgeTTL        time.Duration             `json:"auto_purge_ttl"`
	Kinesis             kinesisbackend.Settings   `json:"kinesis"`
	KMS                 kmsbackend.Settings       `json:"kms"`
	EnforceIAM          bool                      `json:"enforce_iam"`
	Demo                bool                      `json:"demo"`
	Persist             bool                      `json:"persist"`
}

// GetSettings returns the dashboard configuration settings.
func (c *CLI) GetSettings() dashboard.Settings {
	return dashboard.Settings{
		AccountID:           c.AccountID,
		Region:              c.Region,
		LatencyMs:           c.LatencyMs,
		EnforceIAM:          c.EnforceIAM,
		AutoPurgeTTL:        c.AutoPurgeTTL,
		JanitorTimeout:      c.JanitorTimeout,
		LogLevel:            c.LogLevel,
		Port:                c.Port,
		DNSListenAddr:       c.DNSListenAddr,
		DNSResolveIP:        c.DNSResolveIP,
		OpenSearchEngine:    c.OpenSearchEngine,
		ElasticsearchEngine: c.ElasticsearchEngine,
		ElastiCacheEngine:   c.ElastiCacheEngine,
		Persist:             c.Persist,
		Demo:                c.Demo,
		DataDir:             c.DataDir,
		PortRangeStart:      c.PortRangeStart,
		PortRangeEnd:        c.PortRangeEnd,
		InitScriptTimeout:   c.InitScriptTimeout,

		// Service settings
		S3: dashboard.S3Settings{
			DefaultRegion:       c.S3.DefaultRegion,
			JanitorInterval:     c.S3.JanitorInterval,
			CompressionMinBytes: c.S3.CompressionMinBytes,
		},
		Lambda: dashboard.LambdaSettings{
			DockerHost:       c.Lambda.DockerHost,
			ContainerRuntime: c.Lambda.ContainerRuntime,
			PoolSize:         c.Lambda.PoolSize,
			IdleTimeout:      c.Lambda.IdleTimeout,
			MaxRuntimes:      c.Lambda.MaxRuntimes,
		},
		DynamoDB: dashboard.DynamoDBSettings{
			DefaultRegion:     c.DynamoDB.DefaultRegion,
			JanitorInterval:   c.DynamoDB.JanitorInterval,
			CreateDelay:       c.DynamoDB.CreateDelay,
			EnforceThroughput: c.DynamoDB.EnforceThroughput,
		},
		EC2: dashboard.EC2Settings{
			JanitorInterval:  c.EC2.JanitorInterval,
			TerminatedTTL:    c.EC2.TerminatedTTL,
			CancelledSpotTTL: c.EC2.CancelledSpotTTL,
		},
		Backup: dashboard.BackupSettings{
			JanitorInterval: c.Backup.JanitorInterval,
			JobTTL:          c.Backup.JobTTL,
		},
		STS: dashboard.STSSettings{
			JanitorInterval: c.STS.JanitorInterval,
		},
		XRay: dashboard.XRaySettings{
			JanitorInterval: c.XRay.JanitorInterval,
			TraceTTL:        c.XRay.TraceTTL,
		},
		SSM: dashboard.SSMSettings{
			JanitorInterval: c.SSM.JanitorInterval,
			CommandTTL:      c.SSM.CommandTTL,
		},
		CodeBuild: dashboard.CodeBuildSettings{
			JanitorInterval: c.CodeBuild.JanitorInterval,
			BuildTTL:        c.CodeBuild.BuildTTL,
		},
		CloudWatchLogs: dashboard.CloudWatchLogsSettings{
			JanitorInterval:  c.CloudWatchLogs.JanitorInterval,
			MaxRetentionDays: c.CloudWatchLogs.MaxRetentionDays,
		},
		SES: dashboard.SESSettings{
			JanitorInterval: c.SES.JanitorInterval,
			EmailTTL:        c.SES.EmailTTL,
		},
		Batch: dashboard.BatchSettings{
			JanitorInterval:   c.Batch.JanitorInterval,
			InactiveJobDefTTL: c.Batch.InactiveJobDefTTL,
			CompletedJobTTL:   c.Batch.CompletedJobTTL,
		},
		FIS: dashboard.FISSettings{
			JanitorInterval: c.FIS.JanitorInterval,
			ExperimentTTL:   c.FIS.ExperimentTTL,
		},
		EMR: dashboard.EMRSettings{
			JanitorInterval: c.EMR.JanitorInterval,
			TerminatedTTL:   c.EMR.TerminatedTTL,
		},
		Athena: dashboard.AthenaSettings{
			JanitorInterval: c.Athena.JanitorInterval,
			ExecutionTTL:    c.Athena.ExecutionTTL,
		},
		Kinesis: dashboard.KinesisSettings{
			JanitorInterval: c.Kinesis.JanitorInterval,
		},
		KMS: dashboard.KMSSettings{
			JanitorInterval: c.KMS.JanitorInterval,
		},
		StepFunctions: dashboard.StepFunctionsSettings{
			JanitorInterval:    c.StepFunctions.JanitorInterval,
			ExecutionRetention: c.StepFunctions.ExecutionRetention,
		},
	}
}

// UpdateSettings updates both the CLI state and the shared GlobalConfig pointer.
func (c *CLI) UpdateSettings(s dashboard.Settings) {
	c.AccountID = s.AccountID
	c.Region = s.Region
	c.LatencyMs = s.LatencyMs
	c.EnforceIAM = s.EnforceIAM
	c.AutoPurgeTTL = s.AutoPurgeTTL
	c.JanitorTimeout = s.JanitorTimeout
	c.LogLevel = s.LogLevel
	c.Port = s.Port
	c.DNSListenAddr = s.DNSListenAddr
	c.DNSResolveIP = s.DNSResolveIP
	c.OpenSearchEngine = s.OpenSearchEngine
	c.ElasticsearchEngine = s.ElasticsearchEngine
	c.ElastiCacheEngine = s.ElastiCacheEngine
	c.Persist = s.Persist
	c.Demo = s.Demo
	c.DataDir = s.DataDir
	c.PortRangeStart = s.PortRangeStart
	c.PortRangeEnd = s.PortRangeEnd
	c.InitScriptTimeout = s.InitScriptTimeout

	c.applyServiceSettings(s)

	if c.globalConfig != nil {
		c.globalConfig.Update(
			s.AccountID,
			s.Region,
			s.LatencyMs,
			s.JanitorTimeout,
			s.EnforceIAM,
			s.AutoPurgeTTL,
		)
	}
}

// applyServiceSettings copies per-service settings from s into c.
func (c *CLI) applyServiceSettings(s dashboard.Settings) {
	c.S3.DefaultRegion = s.S3.DefaultRegion
	c.S3.JanitorInterval = s.S3.JanitorInterval
	c.S3.CompressionMinBytes = s.S3.CompressionMinBytes

	c.Lambda.DockerHost = s.Lambda.DockerHost
	c.Lambda.ContainerRuntime = s.Lambda.ContainerRuntime
	c.Lambda.PoolSize = s.Lambda.PoolSize
	c.Lambda.IdleTimeout = s.Lambda.IdleTimeout
	c.Lambda.MaxRuntimes = s.Lambda.MaxRuntimes

	c.DynamoDB.DefaultRegion = s.DynamoDB.DefaultRegion
	c.DynamoDB.JanitorInterval = s.DynamoDB.JanitorInterval
	c.DynamoDB.CreateDelay = s.DynamoDB.CreateDelay
	c.DynamoDB.EnforceThroughput = s.DynamoDB.EnforceThroughput

	c.EC2.JanitorInterval = s.EC2.JanitorInterval
	c.EC2.TerminatedTTL = s.EC2.TerminatedTTL
	c.EC2.CancelledSpotTTL = s.EC2.CancelledSpotTTL

	c.Backup.JanitorInterval = s.Backup.JanitorInterval
	c.Backup.JobTTL = s.Backup.JobTTL

	c.STS.JanitorInterval = s.STS.JanitorInterval

	c.XRay.JanitorInterval = s.XRay.JanitorInterval
	c.XRay.TraceTTL = s.XRay.TraceTTL

	c.SSM.JanitorInterval = s.SSM.JanitorInterval
	c.SSM.CommandTTL = s.SSM.CommandTTL

	c.CodeBuild.JanitorInterval = s.CodeBuild.JanitorInterval
	c.CodeBuild.BuildTTL = s.CodeBuild.BuildTTL

	c.CloudWatchLogs.JanitorInterval = s.CloudWatchLogs.JanitorInterval
	c.CloudWatchLogs.MaxRetentionDays = s.CloudWatchLogs.MaxRetentionDays

	c.SES.JanitorInterval = s.SES.JanitorInterval
	c.SES.EmailTTL = s.SES.EmailTTL

	c.Batch.JanitorInterval = s.Batch.JanitorInterval
	c.Batch.InactiveJobDefTTL = s.Batch.InactiveJobDefTTL
	c.Batch.CompletedJobTTL = s.Batch.CompletedJobTTL

	c.FIS.JanitorInterval = s.FIS.JanitorInterval
	c.FIS.ExperimentTTL = s.FIS.ExperimentTTL

	c.EMR.JanitorInterval = s.EMR.JanitorInterval
	c.EMR.TerminatedTTL = s.EMR.TerminatedTTL

	c.Athena.JanitorInterval = s.Athena.JanitorInterval
	c.Athena.ExecutionTTL = s.Athena.ExecutionTTL

	c.Kinesis.JanitorInterval = s.Kinesis.JanitorInterval
	c.KMS.JanitorInterval = s.KMS.JanitorInterval

	c.StepFunctions.JanitorInterval = s.StepFunctions.JanitorInterval
	c.StepFunctions.ExecutionRetention = s.StepFunctions.ExecutionRetention
}

func (c *CLI) SaveConfig() error {
	if c.Demo {
		return nil
	}
	p := filepath.Join(c.resolvedDataDir(), configFilename)
	dir := filepath.Dir(p)

	if err := os.MkdirAll(dir, configDirPerm); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	cfg := PersistableConfig{
		AccountID:           c.AccountID,
		Region:              c.Region,
		LatencyMs:           c.LatencyMs,
		EnforceIAM:          c.EnforceIAM,
		AutoPurgeTTL:        c.AutoPurgeTTL,
		JanitorTimeout:      c.JanitorTimeout,
		LogLevel:            c.LogLevel,
		PortRangeStart:      c.PortRangeStart,
		PortRangeEnd:        c.PortRangeEnd,
		Port:                c.Port,
		DNSListenAddr:       c.DNSListenAddr,
		DNSResolveIP:        c.DNSResolveIP,
		OpenSearchEngine:    c.OpenSearchEngine,
		ElasticsearchEngine: c.ElasticsearchEngine,
		ElastiCacheEngine:   c.ElastiCacheEngine,
		Persist:             c.Persist,
		Demo:                c.Demo,
		DataDir:             c.DataDir,
		InitScriptTimeout:   c.InitScriptTimeout,

		// Service settings
		S3:             c.S3,
		Lambda:         c.Lambda,
		DynamoDB:       c.DynamoDB,
		Backup:         c.Backup,
		STS:            c.STS,
		EC2:            c.EC2,
		XRay:           c.XRay,
		SSM:            c.SSM,
		CodeBuild:      c.CodeBuild,
		CloudWatchLogs: c.CloudWatchLogs,
		SES:            c.SES,
		Batch:          c.Batch,
		FIS:            c.FIS,
		EMR:            c.EMR,
		Athena:         c.Athena,
		Kinesis:        c.Kinesis,
		KMS:            c.KMS,
		StepFunctions:  c.StepFunctions,
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(p, data, configFilePerm)
}

// LoadConfig loads the configuration from disk if it exists.
func (c *CLI) LoadConfig() error {
	if c.Demo {
		return nil
	}
	p := filepath.Join(c.resolvedDataDir(), configFilename)
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed to read config: %w", err)
	}

	var cfg PersistableConfig
	if unmarshalErr := json.Unmarshal(data, &cfg); unmarshalErr != nil {
		return fmt.Errorf("failed to unmarshal config: %w", unmarshalErr)
	}

	// Apply loaded settings to CLI
	c.AccountID = cfg.AccountID
	c.Region = cfg.Region
	c.LatencyMs = cfg.LatencyMs
	c.EnforceIAM = cfg.EnforceIAM
	c.AutoPurgeTTL = cfg.AutoPurgeTTL
	c.JanitorTimeout = cfg.JanitorTimeout
	c.LogLevel = cfg.LogLevel
	c.PortRangeStart = cfg.PortRangeStart
	c.PortRangeEnd = cfg.PortRangeEnd
	c.Port = cfg.Port
	c.DNSListenAddr = cfg.DNSListenAddr
	c.DNSResolveIP = cfg.DNSResolveIP
	c.OpenSearchEngine = cfg.OpenSearchEngine
	c.ElasticsearchEngine = cfg.ElasticsearchEngine
	c.ElastiCacheEngine = cfg.ElastiCacheEngine
	c.Persist = cfg.Persist
	c.Demo = cfg.Demo
	c.DataDir = cfg.DataDir
	c.InitScriptTimeout = cfg.InitScriptTimeout

	// Service settings
	c.S3 = cfg.S3
	c.Lambda = cfg.Lambda
	c.DynamoDB = cfg.DynamoDB
	c.Backup = cfg.Backup
	c.STS = cfg.STS
	c.EC2 = cfg.EC2
	c.XRay = cfg.XRay
	c.SSM = cfg.SSM
	c.CodeBuild = cfg.CodeBuild
	c.CloudWatchLogs = cfg.CloudWatchLogs
	c.SES = cfg.SES
	c.Batch = cfg.Batch
	c.FIS = cfg.FIS
	c.EMR = cfg.EMR
	c.Athena = cfg.Athena
	c.Kinesis = cfg.Kinesis
	c.KMS = cfg.KMS
	c.StepFunctions = cfg.StepFunctions

	return nil
}

// GetAthenaSettings returns Athena settings (athena.ConfigProvider).
func (c *CLI) GetAthenaSettings() athenabackend.Settings {
	return c.Athena
}

// GetBackupSettings returns Backup settings (backup.ConfigProvider).
func (c *CLI) GetBackupSettings() backupbackend.Settings {
	return c.Backup
}

// GetBatchSettings returns Batch settings (batch.ConfigProvider).
func (c *CLI) GetBatchSettings() batchbackend.Settings {
	return c.Batch
}

// GetCloudWatchLogsSettings returns CloudWatch Logs settings (cloudwatchlogs.ConfigProvider).
func (c *CLI) GetCloudWatchLogsSettings() cwlogsbackend.Settings {
	return c.CloudWatchLogs
}

// GetCodeBuildSettings returns CodeBuild settings (codebuild.ConfigProvider).
func (c *CLI) GetCodeBuildSettings() codebuildbackend.Settings {
	return c.CodeBuild
}

// GetEC2Settings returns EC2 settings (ec2.ConfigProvider).
func (c *CLI) GetEC2Settings() ec2backend.Settings {
	return c.EC2
}

// GetEC2ComputeProvider returns the configured EC2 compute provider name
// ("inmemory" or "docker").
func (c *CLI) GetEC2ComputeProvider() string {
	if c.EC2Provider == "" {
		return "inmemory"
	}

	return c.EC2Provider
}

// GetEC2DockerComputeConfig returns the docker provider configuration for the
// EC2 service. The returned value is consumed by ec2.Provider.Init when the
// compute provider is "docker".
func (c *CLI) GetEC2DockerComputeConfig() ec2backend.DockerComputeConfig {
	return ec2backend.DockerComputeConfig{
		Image:      c.EC2DockerImage,
		Network:    c.EC2DockerNetwork,
		SSHHostIP:  c.EC2DockerSSHHostIP,
		Region:     c.Region,
		SSHPortMin: c.EC2DockerSSHPortMin,
		SSHPortMax: c.EC2DockerSSHPortMax,
	}
}

// GetEMRSettings returns EMR settings (emr.ConfigProvider).
func (c *CLI) GetEMRSettings() emrbackend.Settings {
	return c.EMR
}

// GetFISSettings returns FIS settings (fis.ConfigProvider).
func (c *CLI) GetFISSettings() fisbackend.Settings {
	return c.FIS
}

// GetKinesisSettings returns Kinesis settings (kinesis.ConfigProvider).
func (c *CLI) GetKinesisSettings() kinesisbackend.Settings {
	return c.Kinesis
}

// GetSESSettings returns SES settings (ses.ConfigProvider).
func (c *CLI) GetSESSettings() sesbackend.Settings {
	return c.SES
}

// GetXRaySettings returns X-Ray settings (xray.ConfigProvider).
func (c *CLI) GetXRaySettings() xraybackend.Settings {
	return c.XRay
}

// GetDynamoDBClient returns the SDK client for DynamoDB (dashboard.AWSSDKProvider).
func (c *CLI) GetDynamoDBClient() *dynamodb.Client { return c.ddbClient }

// GetS3Client returns the SDK client for S3 (dashboard.AWSSDKProvider).
func (c *CLI) GetS3Client() *s3.Client { return c.s3Client }

// GetSSMClient returns the SDK client for SSM (dashboard.AWSSDKProvider).
func (c *CLI) GetSSMClient() *ssmsdk.Client { return c.ssmClient }

// GetSTSClient returns the SDK client for STS (dashboard.AWSSDKProvider).
func (c *CLI) GetSTSClient() *stssdk.Client { return c.stsClient }

// GetSQSClient returns the SDK client for SQS (dashboard.AWSSDKProvider).
func (c *CLI) GetSQSClient() *sqssdk.Client { return c.sqsClient }

// GetDynamoDBHandler returns the DynamoDB handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetDynamoDBHandler() service.Registerable { return c.ddbHandler }

// GetS3Handler returns the S3 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetS3Handler() service.Registerable { return c.s3Handler }

// GetSSMHandler returns the SSM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSSMHandler() service.Registerable { return c.ssmHandler }

// GetIAMHandler returns the IAM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIAMHandler() service.Registerable { return c.iamHandler }

// GetSTSHandler returns the STS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSTSHandler() service.Registerable { return c.stsHandler }

// GetSNSHandler returns the SNS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSNSHandler() service.Registerable { return c.snsHandler }

// GetSQSHandler returns the SQS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSQSHandler() service.Registerable { return c.sqsHandler }

// GetKMSHandler returns the KMS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKMSHandler() service.Registerable { return c.kmsHandler }

// GetSecretsManagerHandler returns the Secrets Manager handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSecretsManagerHandler() service.Registerable { return c.secretsManagerHandler }

// GetLambdaHandler returns the Lambda handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetLambdaHandler() service.Registerable { return c.lambdaHandler }

// GetEventBridgeHandler returns the EventBridge handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEventBridgeHandler() service.Registerable { return c.eventBridgeHandler }

// GetAPIGatewayHandler returns the API Gateway handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAPIGatewayHandler() service.Registerable { return c.apiGatewayHandler }

// GetCloudWatchLogsHandler returns the CloudWatch Logs handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudWatchLogsHandler() service.Registerable { return c.cloudWatchLogsHandler }

// GetStepFunctionsHandler returns the Step Functions handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetStepFunctionsHandler() service.Registerable { return c.stepFunctionsHandler }

// GetCloudWatchHandler returns the CloudWatch handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudWatchHandler() service.Registerable { return c.cloudWatchHandler }

// GetCloudFormationHandler returns the CloudFormation handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudFormationHandler() service.Registerable { return c.cloudFormationHandler }

// GetKinesisHandler returns the Kinesis handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKinesisHandler() service.Registerable { return c.kinesisHandler }

// GetElastiCacheHandler returns the ElastiCache handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetElastiCacheHandler() service.Registerable { return c.elasticacheHandler }

// GetElastiCacheEngine returns the ElastiCache engine mode (elasticache.EngineConfig).
func (c *CLI) GetElastiCacheEngine() string { return c.ElastiCacheEngine }

// GetRoute53Handler returns the Route 53 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRoute53Handler() service.Registerable { return c.route53Handler }

// GetSESHandler returns the SES handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSESHandler() service.Registerable { return c.sesHandler }

// GetSESv2Handler returns the SES v2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSESv2Handler() service.Registerable { return c.sesv2Handler }

// GetEC2Handler returns the EC2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEC2Handler() service.Registerable { return c.ec2Handler }

// GetElasticsearchEngine returns the Elasticsearch engine mode (elasticsearch.EngineConfig).
func (c *CLI) GetElasticsearchEngine() string { return c.ElasticsearchEngine }

// GetElasticsearchHandler returns the Elasticsearch handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetElasticsearchHandler() service.Registerable { return c.elasticsearchHandler }

// GetOpenSearchEngine returns the OpenSearch engine mode (opensearch.EngineConfig).
func (c *CLI) GetOpenSearchEngine() string { return c.OpenSearchEngine }

// GetOpenSearchHandler returns the OpenSearch handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOpenSearchHandler() service.Registerable { return c.openSearchHandler }

// GetACMHandler returns the ACM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetACMHandler() service.Registerable { return c.acmHandler }

// GetACMPCAHandler returns the ACM PCA handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetACMPCAHandler() service.Registerable { return c.acmpcaHandler }

// GetRedshiftHandler returns the Redshift handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRedshiftHandler() service.Registerable { return c.redshiftHandler }

// GetRedshiftServerlessHandler returns the Redshift Serverless handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRedshiftServerlessHandler() service.Registerable { return c.redshiftServerlessHandler }

// GetRDSHandler returns the RDS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRDSHandler() service.Registerable { return c.rdsHandler }

// GetAWSConfigHandler returns the AWS Config handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAWSConfigHandler() service.Registerable { return c.awsconfigHandler }

// GetS3ControlHandler returns the S3 Control handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetS3ControlHandler() service.Registerable { return c.s3controlHandler }

// GetResourceGroupsHandler returns the Resource Groups handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetResourceGroupsHandler() service.Registerable { return c.resourcegroupsHandler }

// GetResourceGroupsTaggingHandler returns the Resource Groups Tagging API handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetResourceGroupsTaggingHandler() service.Registerable {
	return c.resourcegroupstaggingHandler
}

// GetSWFHandler returns the SWF handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSWFHandler() service.Registerable { return c.swfHandler }

// GetFirehoseHandler returns the Firehose handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetFirehoseHandler() service.Registerable { return c.firehoseHandler }

// GetNetworkMonitorHandler returns the NetworkMonitor handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetNetworkMonitorHandler() service.Registerable { return c.networkmonitorHandler }

// GetSchedulerHandler returns the Scheduler handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSchedulerHandler() service.Registerable { return c.schedulerHandler }

// GetRoute53ResolverHandler returns the Route53Resolver handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRoute53ResolverHandler() service.Registerable { return c.route53resolverHandler }

// GetTranscribeHandler returns the Transcribe handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetTranscribeHandler() service.Registerable { return c.transcribeHandler }

// GetSupportHandler returns the Support handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSupportHandler() service.Registerable { return c.supportHandler }

// GetECRHandler returns the ECR handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetECRHandler() service.Registerable { return c.ecrHandler }

// GetECSHandler returns the ECS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetECSHandler() service.Registerable { return c.ecsHandler }

// GetEFSHandler returns the EFS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEFSHandler() service.Registerable { return c.efsHandler }

// GetEKSHandler returns the EKS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEKSHandler() service.Registerable { return c.eksHandler }

// GetEMRHandler returns the EMR handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEMRHandler() service.Registerable { return c.emrHandler }

// GetGlacierHandler returns the Glacier handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGlacierHandler() service.Registerable { return c.glacierHandler }

// GetIoTAnalyticsHandler returns the IoT Analytics handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIoTAnalyticsHandler() service.Registerable { return c.iotanalyticsHandler }

// GetIoTWirelessHandler returns the IoT Wireless handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIoTWirelessHandler() service.Registerable { return c.iotwirelessHandler }

// GetKinesisAnalyticsHandler returns the Kinesis Analytics handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKinesisAnalyticsHandler() service.Registerable { return c.kinesisanalyticsHandler }

// GetLakeFormationHandler returns the Lake Formation handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetLakeFormationHandler() service.Registerable { return c.lakeformationHandler }

// GetGlueHandler returns the Glue handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGlueHandler() service.Registerable { return c.glueHandler }

// GetGuardDutyHandler returns the GuardDuty handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGuardDutyHandler() service.Registerable { return c.guarddutyHandler }

// GetInspector2Handler returns the Inspector2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetInspector2Handler() service.Registerable { return c.inspector2Handler }

// GetKafkaHandler returns the Kafka handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKafkaHandler() service.Registerable { return c.kafkaHandler }

// GetKinesisAnalyticsV2Handler returns the Kinesis Data Analytics v2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetKinesisAnalyticsV2Handler() service.Registerable {
	return c.kinesisanalyticsv2Handler
}

// GetManagedBlockchainHandler returns the Managed Blockchain handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetManagedBlockchainHandler() service.Registerable { return c.managedblockchainHandler }

// GetMediaConvertHandler returns the MediaConvert handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMediaConvertHandler() service.Registerable { return c.mediaconvertHandler }

// GetMQHandler returns the Amazon MQ handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMQHandler() service.Registerable { return c.mqHandler }

// GetMediaStoreHandler returns the MediaStore handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMediaStoreHandler() service.Registerable { return c.mediastoreHandler }

// GetMediaStoreDataHandler returns the MediaStore Data handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMediaStoreDataHandler() service.Registerable { return c.mediastoredataHandler }

// GetMemoryDBHandler returns the MemoryDB handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMemoryDBHandler() service.Registerable { return c.memorydbHandler }

// GetOrganizationsHandler returns the Organizations handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOrganizationsHandler() service.Registerable { return c.organizationsHandler }

// GetNeptuneHandler returns the Neptune handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetNeptuneHandler() service.Registerable { return c.neptuneHandler }

// GetMWAAHandler returns the MWAA handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetMWAAHandler() service.Registerable { return c.mwaaHandler }

// GetPinpointHandler returns the Pinpoint handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetPinpointHandler() service.Registerable { return c.pinpointHandler }

// GetPipesHandler returns the Pipes handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetPipesHandler() service.Registerable { return c.pipesHandler }

// GetRDSDataHandler returns the RDS Data handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRDSDataHandler() service.Registerable { return c.rdsdataHandler }

// GetRAMHandler returns the RAM handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRAMHandler() service.Registerable { return c.ramHandler }

// GetRedshiftDataHandler returns the Redshift Data handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetRedshiftDataHandler() service.Registerable { return c.redshiftdataHandler }

// GetSageMakerHandler returns the SageMaker handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSageMakerHandler() service.Registerable { return c.sagemakerHandler }

// GetSageMakerRuntimeHandler returns the SageMaker Runtime handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSageMakerRuntimeHandler() service.Registerable { return c.sagemakerRuntimeHandler }

// GetServiceDiscoveryHandler returns the Service Discovery handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetServiceDiscoveryHandler() service.Registerable { return c.servicediscoveryHandler }

// GetServerlessRepoHandler returns the Serverless Application Repository handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetServerlessRepoHandler() service.Registerable { return c.serverlessrepoHandler }

// GetShieldHandler returns the Shield handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetShieldHandler() service.Registerable { return c.shieldHandler }

// GetSsoAdminHandler returns the SSO Admin handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetSsoAdminHandler() service.Registerable { return c.ssoadminHandler }

// GetTextractHandler returns the Textract handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetTextractHandler() service.Registerable { return c.textractHandler }

// GetTimestreamWriteHandler returns the Timestream Write handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetTimestreamWriteHandler() service.Registerable { return c.timestreamwriteHandler }

// GetTimestreamQueryHandler returns the Timestream Query handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetTimestreamQueryHandler() service.Registerable { return c.timestreamqueryHandler }

// GetTransferHandler returns the Transfer handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetTransferHandler() service.Registerable { return c.transferHandler }

// GetVerifiedPermissionsHandler returns the Verified Permissions handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetVerifiedPermissionsHandler() service.Registerable {
	return c.verifiedPermissionsHandler
}

// GetWAFHandler returns the WAF Classic handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetWAFHandler() service.Registerable { return c.wafHandler }

// GetWafv2Handler returns the WAFv2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetWafv2Handler() service.Registerable { return c.wafv2Handler }

// GetXrayHandler returns the X-Ray handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetXrayHandler() service.Registerable { return c.xrayHandler }

// GetS3TablesHandler returns the S3 Tables handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetS3TablesHandler() service.Registerable { return c.s3tablesHandler }

// GetELBHandler returns the ELB handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetELBHandler() service.Registerable { return c.elbHandler }

// GetELBv2Handler returns the ELBv2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetELBv2Handler() service.Registerable { return c.elbv2Handler }

// GetEmrServerlessHandler returns the EMR Serverless handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetEmrServerlessHandler() service.Registerable { return c.emrserverlessHandler }

// GetIoTHandler returns the IoT handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIoTHandler() service.Registerable { return c.iotHandler }

// GetAppSyncHandler returns the AppSync handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAppSyncHandler() service.Registerable { return c.appSyncHandler }

// GetIoTDataPlaneHandler returns the IoT Data Plane handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIoTDataPlaneHandler() service.Registerable { return c.iotDataPlaneHandler }

// GetAPIGatewayManagementAPIHandler returns the API Gateway Management API handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAPIGatewayManagementAPIHandler() service.Registerable {
	return c.apiGatewayMgmtHandler
}

// GetAppConfigDataHandler returns the AppConfigData handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAppConfigDataHandler() service.Registerable {
	return c.appConfigDataHandler
}

// GetAmplifyHandler returns the Amplify handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAmplifyHandler() service.Registerable { return c.amplifyHandler }

// GetAutoscalingHandler returns the Autoscaling handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAutoscalingHandler() service.Registerable { return c.autoscalingHandler }

// GetAPIGatewayV2Handler returns the API Gateway V2 handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAPIGatewayV2Handler() service.Registerable { return c.apiGatewayV2Handler }

// GetAthenaHandler returns the Athena handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAthenaHandler() service.Registerable { return c.athenaHandler }

// GetBackupHandler returns the Backup handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetBackupHandler() service.Registerable { return c.backupHandler }

// GetCloudTrailHandler returns the CloudTrail handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudTrailHandler() service.Registerable { return c.cloudtrailHandler }

// GetAppConfigHandler returns the AppConfig handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAppConfigHandler() service.Registerable { return c.appConfigHandler }

// GetApplicationAutoscalingHandler returns the Application Auto Scaling handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetApplicationAutoscalingHandler() service.Registerable {
	return c.applicationautoscalingHandler
}

// GetBatchHandler returns the Batch handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetBatchHandler() service.Registerable { return c.batchHandler }

// GetBedrockHandler returns the Bedrock handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetBedrockHandler() service.Registerable { return c.bedrockHandler }

// GetBedrockAgentsHandler returns the Bedrock Agents handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetBedrockAgentsHandler() service.Registerable { return c.bedrockAgentsHandler }

// GetBedrockRuntimeHandler returns the Bedrock Runtime handler.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetBedrockRuntimeHandler() service.Registerable { return c.bedrockruntimeHandler }

// GetCeHandler returns the Cost Explorer handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCeHandler() service.Registerable { return c.ceHandler }

// GetCloudControlHandler returns the CloudControl handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (c *CLI) GetCloudControlHandler() service.Registerable { return c.cloudcontrolHandler }

// GetCloudFrontHandler returns the CloudFront handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCloudFrontHandler() service.Registerable { return c.cloudFrontHandler }

// GetCodeArtifactHandler returns the CodeArtifact handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeArtifactHandler() service.Registerable { return c.codeArtifactHandler }

// GetCodeConnectionsHandler returns the CodeConnections handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeConnectionsHandler() service.Registerable { return c.codeConnectionsHandler }

// GetCodeBuildHandler returns the CodeBuild handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeBuildHandler() service.Registerable { return c.codebuildHandler }

// GetCodeCommitHandler returns the CodeCommit handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeCommitHandler() service.Registerable { return c.codeCommitHandler }

// GetCodePipelineHandler returns the CodePipeline handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodePipelineHandler() service.Registerable { return c.codePipelineHandler }

// GetCodeDeployHandler returns the CodeDeploy handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeDeployHandler() service.Registerable { return c.codeDeployHandler }

// GetDMSHandler returns the DMS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetDMSHandler() service.Registerable { return c.dmsHandler }

// GetCodeStarConnectionsHandler returns the CodeStar Connections handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCodeStarConnectionsHandler() service.Registerable {
	return c.codeStarConnectionsHandler
}

// GetDynamoDBStreamsHandler returns the DynamoDB Streams handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetDynamoDBStreamsHandler() service.Registerable {
	return c.dynamodbStreamsHandler
}

// GetElasticbeanstalkHandler returns the Elastic Beanstalk handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetElasticbeanstalkHandler() service.Registerable { return c.elasticbeanstalkHandler }

// GetDocDBHandler returns the DocDB handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetDocDBHandler() service.Registerable { return c.docdbHandler }

// GetFISHandler returns the FIS handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetFISHandler() service.Registerable { return c.fisHandler }

// GetIdentityStoreHandler returns the Identity Store handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetIdentityStoreHandler() service.Registerable { return c.identitystoreHandler }

// GetCognitoIDPHandler returns the Cognito IDP handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCognitoIDPHandler() service.Registerable { return c.cognitoIDPHandler }

// GetCognitoIdentityHandler returns the Cognito Identity handler (dashboard.AWSSDKProvider).
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetCognitoIdentityHandler() service.Registerable { return c.cognitoIdentityHandler }

// GetFaultStore returns the chaos fault store (dashboard.AWSSDKProvider).
func (c *CLI) GetFaultStore() *chaos.FaultStore { return c.faultStore }

// GetPortAllocator returns the shared runtime port allocator.
func (c *CLI) GetPortAllocator() *portalloc.Allocator { return c.portAlloc }

// rootCLI is the top-level kong grammar. The server flags live in Serve
// (the default command); "health" is an explicit subcommand used as a
// Docker healthcheck from scratch containers.
type rootCLI struct {
	Health HealthCmd `cmd:"" help:"Check server health (for Docker healthcheck)."`
	Serve  CLI       `cmd:"" help:"Start the Gopherstack server."                 default:"withargs"`
}

// HealthCmd checks a running Gopherstack instance's health endpoint.
type HealthCmd struct {
	Port string `name:"port" env:"PORT" default:"8000" help:"Port of the running server to check."` //nolint:lll // config struct tags are intentionally verbose
}

var ErrHealthCheckFailed = errors.New("health check failed")

// Run executes the health check. Returns nil on success.
func (h *HealthCmd) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	client := &http.Client{}

	targetURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:" + h.Port,
		Path:   "/_gopherstack/health",
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return fmt.Errorf("create health check request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrHealthCheckFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrHealthCheckFailed, resp.StatusCode)
	}

	fmt.Fprintln(os.Stdout, "ok")

	return nil
}

// Run parses CLI / environment-variable configuration and starts Gopherstack.
// It is called from main() and exits on error.
func Run() {
	var root rootCLI

	kctx := kong.Parse(
		&root,
		kong.Name("gopherstack"),
		kong.Description("In-memory AWS DynamoDB + S3 compatible server."),
	)

	// rootCtx is cancelled when SIGINT/SIGTERM arrives; all subsystems
	// (HTTP server, background workers, DNS server) derive their context
	// from this root so a single signal cleanly unwinds everything.
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	var err error
	switch kctx.Command() {
	case "health":
		err = root.Health.Run()
	default:
		err = run(rootCtx, root.Serve)
	}

	cancel() // release signal-handler goroutine resources

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// applyExplicitOverrides re-applies values from original (parsed from flags/env)
// back onto loaded (merged from persisted config) so that explicit CLI/env settings
// take precedence over persisted values.
// Precedence: defaults < persisted config < env/CLI.
//
// Standard AWS environment variables AWS_DEFAULT_REGION and AWS_REGION are also
// honoured as aliases for the region setting, matching LocalStack and awslocal behaviour.
func applyExplicitOverrides(original, loaded CLI) CLI {
	const defaultAccountID = "000000000000"
	if original.Region != "" && original.Region != defaultRegion {
		loaded.Region = original.Region
	}

	// Also accept the standard AWS SDK region environment variables so that
	// tools using `AWS_DEFAULT_REGION` or `AWS_REGION` work without remapping.
	// AWS_DEFAULT_REGION takes the highest precedence, then AWS_REGION,
	// then the REGION env var / --region flag (already applied above via original.Region).
	if r := os.Getenv("AWS_REGION"); r != "" {
		loaded.Region = r
	}

	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		loaded.Region = r
	}

	if original.Port != "" && original.Port != defaultPort {
		loaded.Port = original.Port
	}
	if original.AccountID != "" && original.AccountID != defaultAccountID {
		loaded.AccountID = original.AccountID
	}
	if original.AutoPurgeTTL != 0 {
		loaded.AutoPurgeTTL = original.AutoPurgeTTL
	}

	return loaded
}

// setupPortAllocator creates a port allocator from the given range.
// Returns nil when the range is invalid (allocator disabled).
func setupPortAllocator(
	ctx context.Context,
	log *slog.Logger,
	start, end int,
) *portalloc.Allocator {
	alloc, err := portalloc.New(start, end)
	if err != nil {
		log.WarnContext(ctx, "Port allocator disabled (invalid range)", "error", err)

		return nil
	}
	log.InfoContext(
		ctx,
		"Port allocator ready",
		"start",
		start,
		"end",
		end,
		"available",
		alloc.Available(),
	)

	return alloc
}

// run starts the server with the given CLI configuration.
// It is separated from Run so it can be exercised in tests without [os.Exit].
func run(ctx context.Context, cli CLI) error {
	log := buildLogger(cli.LogLevel)
	ctx = logger.Save(ctx, log)

	// Take a snapshot of the CLI values as parsed from flags/env so that explicit
	// CLI/env settings take precedence over any persisted config.
	original := cli

	if cli.Persist {
		if err := cli.LoadConfig(); err != nil {
			log.WarnContext(ctx, "Failed to load config", "error", err)
		}
	}

	cli = applyExplicitOverrides(original, cli)
	cli.GetGlobalConfig().Update(
		cli.AccountID, cli.Region, cli.LatencyMs,
		cli.JanitorTimeout, cli.EnforceIAM, cli.AutoPurgeTTL,
	)

	// --- Port allocator ---
	cli.portAlloc = setupPortAllocator(ctx, log, cli.PortRangeStart, cli.PortRangeEnd)

	// --- Embedded DNS server ---
	var dnsSrv *gopherDNS.Server
	if cli.DNSListenAddr != "" {
		dnsSrv = startEmbeddedDNS(ctx, cli.DNSListenAddr, cli.DNSResolveIP)
	}

	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfgVal, err := buildInternalAWSConfig(ctx, cli.Region, inMemClient)
	if err != nil {
		log.ErrorContext(ctx, "Failed to load AWS config", "error", err)

		return err
	}

	initializeClients(&cli, awsCfgVal)

	janitorCtx, janitorCancel := context.WithCancel(ctx)
	defer janitorCancel() // also passed to shutdownBackends so janitors stop before backends are torn down

	// --- Persistence ---
	persistManager, err := initPersistenceManager(ctx, &cli)
	if err != nil {
		return err
	}

	if cli.Persist {
		defer persistManager.SaveAll(ctx)
	}

	appCtx := &service.AppContext{
		Logger:         log,
		Config:         &cli,
		JanitorCtx:     janitorCtx,
		JanitorTimeout: cli.JanitorTimeout,
		PortAlloc:      cli.portAlloc,
	}

	// Create the fault store before initialising services so the dashboard can
	// receive it via cli.GetFaultStore() during its Init() call.
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	if err != nil {
		return err
	}

	setupPersistence(ctx, persistManager, services, cli.Persist)

	if dnsSrv != nil {
		wireDNSRegistrars(&cli, dnsSrv)
	}

	e := buildEchoServer(ctx, log, persistManager, services, cli)

	if setupErr := setupChaosAndRegistry(e, log, &cli, services); setupErr != nil {
		return setupErr
	}

	startBackgroundWorkers(janitorCtx, services)

	// Start automatic purge background worker. It dynamically checks for TTL updates from the config.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "purge worker: panic recovered",
					"panic", fmt.Sprintf("%v", r))
			}
		}()
		startPurgeWorker(janitorCtx, log, cli.globalConfig, services)
	}()

	inMemMux.Handle("/", e)

	if cli.Demo {
		loadDemoData(ctx, &cli)
	}

	runInitHooks(ctx, &cli, log)
	createS3InitBuckets(ctx, &cli, log)
	defer shutdownBackends(janitorCancel, cli.lambdaHandler, services)

	return startServer(ctx, cli.Port, e, tlsConfigFromCLI(&cli))
}

// tlsSettings carries the resolved TLS configuration for the listener.
type tlsSettings struct {
	// certFile / keyFile point to PEM files; when both empty (and enabled), a
	// self-signed certificate is generated in-memory on startup.
	certFile string
	keyFile  string
	// enabled is true when the server should serve HTTPS.
	enabled bool
}

// tlsConfigFromCLI derives the TLS listener settings from CLI flags. TLS is
// enabled when --tls is set or when an explicit cert/key pair is supplied.
func tlsConfigFromCLI(cli *CLI) tlsSettings {
	enabled := cli.TLS || (cli.TLSCertFile != "" && cli.TLSKeyFile != "")

	return tlsSettings{
		enabled:  enabled,
		certFile: cli.TLSCertFile,
		keyFile:  cli.TLSKeyFile,
	}
}

// runInitHooks runs init scripts after all services are ready, if any are configured.
func runInitHooks(ctx context.Context, cli *CLI, log *slog.Logger) {
	if len(cli.InitScripts) == 0 {
		return
	}

	runner := inithooks.New(cli.InitScripts, cli.InitScriptTimeout, log)
	runner.Run(ctx)
}

// createS3InitBuckets creates S3 buckets listed in cli.S3InitBuckets on startup.
// Bucket names may be passed individually (--s3-bucket name) or as a comma-separated
// list (S3_BUCKETS=a,b,c), matching the awslocal convention of `awslocal s3 mb s3://name`.
// Errors are logged but do not abort server startup.
func createS3InitBuckets(ctx context.Context, cli *CLI, log *slog.Logger) {
	if len(cli.S3InitBuckets) == 0 {
		return
	}

	// Flatten comma-separated entries that may come from the S3_BUCKETS env var.
	var buckets []string

	for _, entry := range cli.S3InitBuckets {
		for name := range strings.SplitSeq(entry, ",") {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				buckets = append(buckets, trimmed)
			}
		}
	}

	for _, bucket := range buckets {
		if _, err := cli.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		}); err != nil {
			log.WarnContext(ctx, "Failed to create init S3 bucket", "bucket", bucket, "error", err)
		} else {
			log.InfoContext(ctx, "Created S3 bucket on startup", "bucket", bucket)
		}
	}
}

// buildInternalAWSConfig constructs an [aws.Config] for the server's own SDK clients.
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are used when set, so that tooling that
// configures these standard env vars (e.g. awslocal) works without credential remapping.
// The server never validates incoming credentials, so the exact values do not matter.
func buildInternalAWSConfig(
	ctx context.Context,
	region string,
	httpClient awscfg.HTTPClient,
) (aws.Config, error) {
	keyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if keyID == "" {
		keyID = "dummy"
	}

	if secret == "" {
		secret = "dummy"
	}

	return awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(keyID, secret, "")),
		awscfg.WithHTTPClient(httpClient),
	)
}

// lambdaCloseFn returns a cleanup function that shuts down the Lambda backend's
// function URL servers and runtime API servers, or nil if the handler is not a Lambda backend.
func lambdaCloseFn(lambdaReg service.Registerable) func() {
	lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler)
	if !lambdaOk {
		return nil
	}

	lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !bkOk {
		return nil
	}

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		lambdaBk.Close(ctx)
	}
}

// shutdownBackends cancels background workers, shuts down the Lambda backend,
// and then shuts down every service that implements service.Shutdowner. It is
// called via defer after the HTTP server has stopped accepting requests.
// janitorCancel is called first so that janitor goroutines stop before the
// backends they access are torn down.
func shutdownBackends(
	janitorCancel context.CancelFunc,
	lambdaHandler service.Registerable,
	services []service.Registerable,
) {
	// Stop janitor workers before closing backends they may still be accessing.
	janitorCancel()

	if closeFn := lambdaCloseFn(lambdaHandler); closeFn != nil {
		closeFn()
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutCancel()

	shutdownServices(shutCtx, services)
}

// wireDNSRegistrars connects DNS-aware backends to the embedded DNS server.
func wireDNSRegistrars(cli *CLI, dnsSrv *gopherDNS.Server) {
	wireLambdaDNS(cli.lambdaHandler, dnsSrv)
	wireRoute53DNS(cli.route53Handler, dnsSrv)
	wireRDSDNS(cli.rdsHandler, dnsSrv)
	wireRedshiftDNS(cli.redshiftHandler, dnsSrv)
	wireElasticsearchDNS(cli.elasticsearchHandler, dnsSrv)
	wireOpenSearchDNS(cli.openSearchHandler, dnsSrv)
	wireElastiCacheDNS(cli.elasticacheHandler, dnsSrv)
	wireEC2DNS(cli.ec2Handler, dnsSrv)
}

// buildEchoServer creates and configures the Echo HTTP server.
func buildEchoServer(
	ctx context.Context,
	log *slog.Logger,
	persistManager *persistence.Manager,
	services []service.Registerable,
	cli CLI,
) *echo.Echo {
	e := echo.New()
	e.Use(panicRecoveryMiddleware())
	e.Use(httputils.RequestIDMiddleware())
	e.Use(logger.APIConsoleMiddleware())
	e.Use(telemetry.MemoryStatsMiddleware)
	e.Pre(logger.EchoMiddleware(log))

	// Optional, opt-in SigV4 signature validation. Off by default so existing
	// clients (which sign with dummy creds) are not rejected.
	if cli.ValidateSigV4 {
		log.InfoContext(ctx, "SigV4 request-signature validation ENABLED")
		e.Use(httputils.NewSigV4Validator(cli.SigV4Secret).EchoMiddleware())
	}

	e.HTTPErrorHandler = buildHTTPErrorHandler()
	e.GET("/_gopherstack/health", buildHealthHandler(services))
	e.POST("/_gopherstack/reset", buildResetHandler(services))
	e.POST("/_gopherstack/snapshot", buildSnapshotHandler(persistManager))
	e.POST("/_gopherstack/load", buildLoadHandler(persistManager))

	registerWebsiteRoutes(e, services)

	if cli.Persist {
		e.Use(persistenceMiddleware(persistManager, services))
	}

	return e
}

// buildHTTPErrorHandler returns an Echo error handler that logs 5xx errors via
// slog and writes the standard JSON error response.
func buildHTTPErrorHandler() func(*echo.Context, error) {
	return func(c *echo.Context, err error) {
		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			httpErr = echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		if httpErr.Code >= http.StatusInternalServerError {
			logger.Load(c.Request().Context()).ErrorContext(
				c.Request().Context(), "HTTP error",
				"status", httpErr.Code,
				"error", httpErr.Message,
				"path", c.Request().URL.Path,
				"method", c.Request().Method,
			)
		}

		if resp, _ := echo.UnwrapResponse(c.Response()); resp == nil || !resp.Committed {
			_ = c.JSON(httpErr.Code, map[string]any{keyMessageField: httpErr.Message})
		}
	}
}

// buildHealthHandler returns the /_gopherstack/health handler. It reports service
// names and runtime memory/goroutine stats.
func buildHealthHandler(services []service.Registerable) echo.HandlerFunc {
	return func(c *echo.Context) error {
		names := make([]string, 0, len(services))
		for _, svc := range services {
			names = append(names, svc.Name())
		}

		sort.Strings(names)

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		return c.JSON(http.StatusOK, healthResponse{
			Status:     "ok",
			Version:    version.Get(),
			Services:   names,
			Goroutines: runtime.NumGoroutine(),
			HeapAllocB: ms.HeapAlloc,
			HeapInuseB: ms.HeapInuse,
			NumGC:      ms.NumGC,
		})
	}
}

// buildResetHandler returns the /_gopherstack/reset handler that clears all
// in-memory state for every service that supports it.
func buildResetHandler(services []service.Registerable) echo.HandlerFunc {
	return func(c *echo.Context) error {
		reset := 0
		reqService := c.QueryParam("service")

		for _, svc := range services {
			if r, ok := svc.(service.Resettable); ok {
				if reqService != "" && !strings.EqualFold(reqService, svc.Name()) {
					continue
				}
				r.Reset()
				reset++
			}
		}

		return c.JSON(http.StatusOK, map[string]any{
			"status":        "ok",
			"reset":         reset,
			keyMessageField: fmt.Sprintf("reset %d service(s)", reset),
		})
	}
}

// snapshotBundle is the JSON envelope returned by POST /_gopherstack/snapshot
// and accepted by POST /_gopherstack/load.
//
// Each value in Services is a raw JSON blob produced by the service's
// Snapshot() method, embedded verbatim so that round-trips are lossless and the
// payload stays human-readable without an extra base64 layer.
type snapshotBundle struct {
	// Services maps each registered service name to its raw JSON snapshot.
	Services map[string]json.RawMessage `json:"services"`
	// Format is a fixed version tag; currently "gopherstack-snapshot/v1".
	Format string `json:"format"`
}

// snapshotResponse is the JSON body returned by POST /_gopherstack/snapshot.
type snapshotResponse struct {
	snapshotBundle
	Status string `json:"status"`
	// Exported is the number of services whose snapshots were included.
	Exported int `json:"exported"`
}

// loadResponse is the JSON body returned by POST /_gopherstack/load.
type loadResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	// Loaded is the number of services successfully restored.
	Loaded int `json:"loaded"`
}

// snapshotBundleFormat is the format identifier embedded in every snapshot bundle.
const snapshotBundleFormat = "gopherstack-snapshot/v1"

// buildSnapshotHandler returns the POST /_gopherstack/snapshot handler.
//
// It calls ExportAll on the persistence manager to collect the current
// in-memory state of every registered service, wraps the snapshots in a
// snapshotBundle envelope, and returns it as the response body. The caller
// can store the blob and later replay it via POST /_gopherstack/load to
// restore state (Cloud-Pods style).
//
// The handler does not write to the underlying Store; if the caller also
// wants on-disk persistence they should use --persist/--data-dir together
// with the debounced auto-snapshot that fires after every mutation.
func buildSnapshotHandler(m *persistence.Manager) echo.HandlerFunc {
	return func(c *echo.Context) error {
		raw := m.ExportAll()

		services := make(map[string]json.RawMessage, len(raw))
		for name, data := range raw {
			services[name] = json.RawMessage(data)
		}

		resp := snapshotResponse{
			snapshotBundle: snapshotBundle{
				Format:   snapshotBundleFormat,
				Services: services,
			},
			Exported: len(services),
			Status:   "ok",
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// buildLoadHandler returns the POST /_gopherstack/load handler.
//
// It reads a snapshotBundle from the request body and restores each service's
// state by calling ImportAll on the persistence manager. Services present in
// the bundle but not registered with the manager are warned and skipped.
// Returns 400 for malformed input and 500 if any restore fails.
func buildLoadHandler(m *persistence.Manager) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var bundle snapshotBundle

		if err := json.NewDecoder(c.Request().Body).Decode(&bundle); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest,
				fmt.Sprintf("invalid snapshot bundle: %s", err.Error()))
		}

		snapshots := make(map[string][]byte, len(bundle.Services))
		for name, raw := range bundle.Services {
			snapshots[name] = []byte(raw)
		}

		ctx := c.Request().Context()

		if err := m.ImportAll(ctx, snapshots); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,
				fmt.Sprintf("restore failed: %s", err.Error()))
		}

		resp := loadResponse{
			Status:  "ok",
			Loaded:  len(snapshots),
			Message: fmt.Sprintf("restored %d service(s)", len(snapshots)),
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// registerWebsiteRoutes registers the S3 website-serving routes on e.
// It finds the S3 handler from the registered services list.
func registerWebsiteRoutes(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		s3H, ok := svc.(*s3backend.S3Handler)
		if !ok {
			continue
		}

		e.GET("/_gopherstack/website/:bucket/*", s3H.ServeWebsite)
		e.GET("/_gopherstack/website/:bucket", func(c *echo.Context) error {
			bucket := c.Param("bucket")

			return c.Redirect(http.StatusMovedPermanently, "/_gopherstack/website/"+bucket+"/")
		})

		break
	}
}

// initializeClients configures the AWS SDK clients for DynamoDB, S3, SSM, and STS.
func initializeClients(cli *CLI, awsCfg aws.Config) {
	cli.ddbClient = dynamodb.NewFromConfig(
		awsCfg,
		func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.s3Client = s3.NewFromConfig(
		awsCfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.ssmClient = ssmsdk.NewFromConfig(
		awsCfg,
		func(o *ssmsdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.stsClient = stssdk.NewFromConfig(
		awsCfg,
		func(o *stssdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.sqsClient = sqssdk.NewFromConfig(
		awsCfg,
		func(o *sqssdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.snsClient = sns.NewFromConfig(
		awsCfg,
		func(o *sns.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.iamClient = iam.NewFromConfig(
		awsCfg,
		func(o *iam.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.kmsClient = kms.NewFromConfig(
		awsCfg,
		func(o *kms.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.secretsManagerClient = secretsmanager.NewFromConfig(
		awsCfg,
		func(o *secretsmanager.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.ecrClient = ecr.NewFromConfig(
		awsCfg,
		func(o *ecr.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.appSyncSdkClient = appsyncsdksvc.NewFromConfig(
		awsCfg,
		func(o *appsyncsdksvc.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.amplifyClient = amplifysdk.NewFromConfig(
		awsCfg,
		func(o *amplifysdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.ecsClient = ecs.NewFromConfig(
		awsCfg,
		func(o *ecs.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.eksClient = ekssdk.NewFromConfig(
		awsCfg,
		func(o *ekssdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	initializeIoTAndCodeClients(cli, awsCfg)
}

// initializeIoTAndCodeClients configures IoT, CodeDeploy and CodePipeline SDK clients.
func initializeIoTAndCodeClients(cli *CLI, awsCfg aws.Config) {
	cli.iotClient = iotsdk.NewFromConfig(
		awsCfg,
		func(o *iotsdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.codeDeployClient = codedeploysdk.NewFromConfig(
		awsCfg,
		func(o *codedeploysdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
	cli.codePipelineSDKClient = codepipelinesdk.NewFromConfig(
		awsCfg,
		func(o *codepipelinesdk.Options) {
			o.BaseEndpoint = aws.String("http://local")
		},
	)
}

// serviceByName builds a lookup map from service Name() to the service instance.
func serviceByName(services []service.Registerable) map[string]service.Registerable {
	m := make(map[string]service.Registerable, len(services))
	for _, svc := range services {
		m[svc.Name()] = svc
	}

	return m
}

// storeCLIHandlers assigns initialized service handlers to the CLI fields using name-based lookup.
func storeCLIHandlers(cli *CLI, services []service.Registerable) {
	byName := serviceByName(services)

	cli.ddbHandler = byName["DynamoDB"]
	cli.s3Handler = byName["S3"]
	cli.ssmHandler = byName["SSM"]
	cli.iamHandler = byName["IAM"]
	cli.stsHandler = byName["STS"]
	cli.snsHandler = byName["SNS"]
	cli.sqsHandler = byName["SQS"]
	cli.kmsHandler = byName["KMS"]
	cli.secretsManagerHandler = byName["SecretsManager"]
	cli.lambdaHandler = byName["Lambda"]
	cli.eventBridgeHandler = byName["EventBridge"]
	cli.apiGatewayHandler = byName["APIGateway"]
	cli.cloudWatchLogsHandler = byName["CloudWatchLogs"]
	cli.stepFunctionsHandler = byName["StepFunctions"]
	cli.cloudWatchHandler = byName["CloudWatch"]
	cli.kinesisHandler = byName["Kinesis"]
	cli.elasticacheHandler = byName["ElastiCache"]
	cli.route53Handler = byName["Route53"]
	cli.sesHandler = byName["SES"]
	cli.sesv2Handler = byName["SESv2"]
	cli.ec2Handler = byName["EC2"]
	cli.elasticsearchHandler = byName["Elasticsearch"]
	cli.openSearchHandler = byName["OpenSearch"]
	cli.acmHandler = byName["ACM"]
	cli.acmpcaHandler = byName["ACMPCA"]
	cli.redshiftHandler = byName["Redshift"]
	cli.redshiftServerlessHandler = byName["RedshiftServerless"]
	cli.awsconfigHandler = byName["AWSConfig"]
	cli.s3controlHandler = byName["S3Control"]
	cli.resourcegroupsHandler = byName["ResourceGroups"]
	cli.resourcegroupstaggingHandler = byName["ResourceGroupsTaggingAPI"]
	cli.swfHandler = byName["SWF"]
	cli.firehoseHandler = byName["Firehose"]
	cli.networkmonitorHandler = byName["NetworkMonitor"]
	cli.schedulerHandler = byName["Scheduler"]
	cli.route53resolverHandler = byName["Route53Resolver"]
	cli.rdsHandler = byName["RDS"]
	cli.transcribeHandler = byName["Transcribe"]
	cli.supportHandler = byName["Support"]
	cli.appSyncHandler = byName["AppSync"]

	storeCLIRecentHandlers(cli, byName)
}

// storeCLIRecentHandlers assigns recently-added service handlers to the CLI fields.
func storeCLIRecentHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.iotDataPlaneHandler = byName["IoTDataPlane"]
	cli.apiGatewayMgmtHandler = byName["APIGatewayManagementAPI"]

	storeAdditionalCLIHandlers(cli, byName)
}

// storeAdditionalCLIHandlers stores recently-added service handlers into the CLI struct.
func storeAdditionalCLIHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.appConfigDataHandler = byName["AppConfigData"]
	cli.amplifyHandler = byName["Amplify"]
	cli.autoscalingHandler = byName["Autoscaling"]
	cli.apiGatewayV2Handler = byName["APIGatewayV2"]
	storeCLIExtendedHandlers(cli, byName)
}

// storeCLIExtendedHandlers assigns handlers for services added after the initial set.
func storeCLIExtendedHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.athenaHandler = byName["Athena"]
	cli.appConfigHandler = byName["AppConfig"]
	cli.applicationautoscalingHandler = byName["ApplicationAutoscaling"]
	cli.batchHandler = byName["Batch"]
	cli.bedrockHandler = byName["Bedrock"]
	cli.bedrockAgentsHandler = byName["BedrockAgents"]
	cli.bedrockruntimeHandler = byName["BedrockRuntime"]
	cli.ecrHandler = byName["ECR"]
	cli.ecsHandler = byName["ECS"]
	cli.iotHandler = byName["IoT"]
	cli.cognitoIDPHandler = byName["CognitoIDP"]
	cli.cognitoIdentityHandler = byName["CognitoIdentity"]
	cli.fisHandler = byName["FIS"]
	cli.identitystoreHandler = byName["IdentityStore"]
	cli.backupHandler = byName["Backup"]
	cli.cloudtrailHandler = byName["CloudTrail"]
	cli.ceHandler = byName["Ce"]
	cli.cloudcontrolHandler = byName["CloudControl"]
	cli.cloudFrontHandler = byName["CloudFront"]
	cli.codeArtifactHandler = byName["CodeArtifact"]
	cli.codeConnectionsHandler = byName["CodeConnections"]
	cli.codebuildHandler = byName["CodeBuild"]
	cli.codeCommitHandler = byName["CodeCommit"]
	cli.codePipelineHandler = byName["CodePipeline"]
	cli.codeDeployHandler = byName["CodeDeploy"]
	cli.dmsHandler = byName["DMS"]
	cli.codeStarConnectionsHandler = byName["CodeStarConnections"]
	cli.dynamodbStreamsHandler = byName["DynamoDBStreams"]
	cli.elasticbeanstalkHandler = byName["Elasticbeanstalk"]
	cli.efsHandler = byName["EFS"]
	cli.eksHandler = byName["EKS"]
	cli.elbHandler = byName["ELB"]
	cli.elbv2Handler = byName["ELBv2"]
	cli.emrserverlessHandler = byName["EmrServerless"]
	cli.emrHandler = byName["EMR"]
	storeCLILatestHandlers(cli, byName)
}

// storeCLILatestHandlers assigns the newest service handlers to the CLI fields.
func storeCLILatestHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.glacierHandler = byName["Glacier"]
	cli.iotwirelessHandler = byName["IoTWireless"]
	cli.kinesisanalyticsHandler = byName["KinesisAnalytics"]
	cli.lakeformationHandler = byName["LakeFormation"]
	cli.glueHandler = byName["Glue"]
	cli.guarddutyHandler = byName["GuardDuty"]
	cli.inspector2Handler = byName["Inspector2"]
	cli.iotanalyticsHandler = byName["IoTAnalytics"]
	cli.kafkaHandler = byName["Kafka"]
	cli.kinesisanalyticsv2Handler = byName["KinesisAnalyticsV2"]
	cli.managedblockchainHandler = byName["ManagedBlockchain"]
	cli.mediaconvertHandler = byName["MediaConvert"]
	cli.mqHandler = byName["MQ"]
	cli.mediastoreHandler = byName["MediaStore"]
	cli.mediastoredataHandler = byName["MediaStoreData"]
	storeCLINewestHandlers(cli, byName)
}

// storeCLINewestHandlers assigns handlers for the most recently added services.
func storeCLINewestHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.memorydbHandler = byName["MemoryDB"]
	cli.organizationsHandler = byName["Organizations"]
	cli.mwaaHandler = byName["MWAA"]
	cli.neptuneHandler = byName["Neptune"]
	cli.docdbHandler = byName["DocDB"]
	cli.pinpointHandler = byName["Pinpoint"]
	cli.pipesHandler = byName["Pipes"]
	cli.rdsdataHandler = byName["RDSData"]
	cli.ramHandler = byName["RAM"]
	cli.redshiftdataHandler = byName["RedshiftData"]
	cli.sagemakerHandler = byName["SageMaker"]
	cli.sagemakerRuntimeHandler = byName["SageMakerRuntime"]
	cli.servicediscoveryHandler = byName["ServiceDiscovery"]
	cli.serverlessrepoHandler = byName["ServerlessRepo"]
	cli.shieldHandler = byName["Shield"]
	cli.ssoadminHandler = byName["SsoAdmin"]
	cli.textractHandler = byName["Textract"]
	cli.comprehendHandler = byName["Comprehend"]
	cli.timestreamwriteHandler = byName["TimestreamWrite"]
	cli.timestreamqueryHandler = byName["TimestreamQuery"]
	cli.transferHandler = byName["Transfer"]
	cli.verifiedPermissionsHandler = byName["VerifiedPermissions"]
	cli.wafHandler = byName["WAF"]
	cli.wafv2Handler = byName["Wafv2"]
	cli.xrayHandler = byName["Xray"]
	cli.s3tablesHandler = byName["S3tables"]
}

// initializeServices initializes all service providers.
//
//nolint:funlen // registers many services; splitting would obscure the dependency wiring
func initializeServices(appCtx *service.AppContext) ([]service.Registerable, error) {
	providers := getServiceProviders()
	services := make([]service.Registerable, 0, len(providers))

	for _, provider := range providers {
		svc, err := provider.Init(appCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to init %s: %w", provider.Name(), err)
		}

		services = append(services, svc)
	}

	// Store handlers in CLI so dashboard and CloudFormation can access them.
	if cli, ok := appCtx.Config.(*CLI); ok {
		storeCLIHandlers(cli, services)
	}

	// Build name-based lookup for cross-service wiring.
	byName := serviceByName(services)

	// Wire SNS→SQS delivery: when SNS publishes a message, deliver it to SQS queues.
	wireSNSToSQS(byName["SNS"], byName["SQS"])

	// Wire SQS → CloudWatch metric emission for NumberOfMessagesSent/Received/Deleted.
	wireSQSMetrics(byName["SQS"], byName["CloudWatch"])

	// Wire EventBridge target fan-out: deliver events to Lambda, SQS, SNS targets.
	wireEventBridgeDelivery(byName["EventBridge"], byName["Lambda"], byName["SQS"], byName["SNS"])

	// Wire S3 bucket notification delivery to SQS/SNS/Lambda targets.
	wireS3Notifications(
		byName["S3"],
		byName["SQS"],
		byName["SNS"],
		byName["Lambda"],
		byName["EventBridge"],
	)

	// Wire Step Functions → Lambda Task integration.
	wireStepFunctionsLambda(byName["StepFunctions"], byName["Lambda"])

	// Wire Step Functions → SQS/SNS/DynamoDB service integrations.
	wireStepFunctionsServiceIntegrations(
		byName["StepFunctions"],
		byName["SQS"],
		byName["SNS"],
		byName["DynamoDB"],
	)

	// Wire SSM → KMS for SecureString encryption with customer-managed keys.
	wireSSMKMS(byName["SSM"], byName["KMS"])

	// Wire API Gateway → Lambda proxy integration.
	wireAPIGatewayLambda(byName["APIGateway"], byName["Lambda"])

	// Wire Kinesis → Lambda event source mapping poller.
	wireKinesisLambda(byName["Kinesis"], byName["Lambda"])

	// Wire SQS → Lambda event source mapping poller.
	wireSQSLambda(byName["SQS"], byName["Lambda"])

	// Wire DynamoDB Streams → Lambda event source mapping poller.
	wireDynamoDBStreamLambda(byName["DynamoDB"], byName["Lambda"])

	// Wire CloudWatch alarm actions → SNS and Lambda backends.
	wireCloudWatchAlarmActions(byName["CloudWatch"], byName["SNS"], byName["Lambda"])

	// Wire CloudWatch Logs → Lambda log delivery.
	wireLambdaCWLogs(byName["Lambda"], byName["CloudWatchLogs"])

	// Wire CloudWatch Logs subscription filter delivery to Lambda, Kinesis, and Firehose.
	wireCWLogsSubscriptionFilters(
		byName["CloudWatchLogs"],
		byName["Lambda"],
		byName["Kinesis"],
		byName["Firehose"],
	)

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire CloudWatch Logs metric filters to emit CloudWatch metric data points.
	wireCWLogsMetricEmitter(byName["CloudWatchLogs"], byName["CloudWatch"])

	// Wire Firehose → S3 and Lambda for actual record delivery and transformation.
	wireFirehoseDelivery(byName["Firehose"], byName["S3"], byName["Lambda"])

	// Wire Lambda invoker → SecretsManager rotation.
	wireSecretsManagerLambda(byName["SecretsManager"], byName["Lambda"])

	// Wire IoT rules → SQS/Lambda action dispatch, and broker → IoT Data Plane.
	wireIoTRules(byName["IoT"], byName["IoTDataPlane"], byName["SQS"], byName["Lambda"])

	// Wire AppSync → Lambda for LAMBDA resolver execution.
	wireAppSyncLambda(byName["AppSync"], byName["Lambda"])

	// Wire AppSync → DynamoDB for AMAZON_DYNAMODB resolver execution.
	wireAppSyncDynamoDB(byName["AppSync"], byName["DynamoDB"])

	// Wire DynamoDB Streams → DynamoDB backend so streams share the same in-memory data.
	wireDynamoDBStreams(byName["DynamoDB"], byName["DynamoDBStreams"])

	// Wire Scheduler runner → Lambda, SQS, SNS, and StepFunctions backends.
	wireSchedulerRunner(
		byName["Scheduler"],
		byName["Lambda"],
		byName["SQS"],
		byName["SNS"],
		byName["StepFunctions"],
	)

	// Wire Pipes runner → SQS (source), Lambda, and StepFunctions (targets).
	wirePipesRunner(byName["Pipes"], byName["SQS"], byName["Lambda"], byName["StepFunctions"])

	// Wire Resource Groups Tagging API → service backends so GetResources, TagResources, etc.
	// aggregate and mutate tags across all services.
	wireResourceGroupsTagging(
		byName["ResourceGroupsTaggingAPI"],
		byName["DynamoDB"],
		byName["SQS"],
		byName["SNS"],
		byName["Lambda"],
		byName["KMS"],
		byName["SecretsManager"],
	)

	// Wire IAM → STS for ExternalId validation and MaxSessionDuration enforcement.
	wireIAMToSTS(byName["IAM"], byName["STS"])

	// Collect all services implementing FISActionProvider and register them with the FIS backend.
	wireFISActionProviders(byName["FIS"], services)

	// Init CloudFormation after core handlers are stored so it can access their backends.
	cfnSvc, err := (&cfnbackend.Provider{}).Init(appCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init CloudFormation: %w", err)
	}

	services = append(services, cfnSvc)

	if cli, ok := appCtx.Config.(*CLI); ok {
		cli.cloudFormationHandler = cfnSvc
	}

	// Init dashboard last so it can access all service handlers.
	dashSvc, err := (&dashboard.Provider{}).Init(appCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init Dashboard: %w", err)
	}
	services = append(services, dashSvc)

	// Record the service count so the metrics dashboard (and Prometheus scrape)
	// can surface it alongside the health endpoint's "services" field.
	telemetry.SetServiceCount(len(services))

	// The router sorts services by MatchPriority() at startup, so registration order
	// does not affect routing correctness.
	return services, nil
}

// getServiceProviders returns the list of all available service providers.
func getServiceProviders() []service.Provider {
	return append(getCoreServiceProviders(), getRemainingServiceProviders()...)
}

// getCoreServiceProviders returns the foundational service providers.
// Extracted from getServiceProviders to satisfy the funlen limit and to
// give the inline registration list headroom as new services are added.
func getCoreServiceProviders() []service.Provider {
	return []service.Provider{
		&ddbbackend.Provider{},
		&s3backend.Provider{},
		&ssmbackend.Provider{},
		&iambackend.Provider{},
		&stsbackend.Provider{},
		&snsbackend.Provider{},
		&sqsbackend.Provider{},
		&kmsbackend.Provider{},
		&secretsmanagerbackend.Provider{},
		&lambdabackend.Provider{},
		&ebbackend.Provider{},
	}
}

// getRemainingServiceProviders returns the remaining service providers. New
// services should be registered in getMostRecentServiceProviders (the tail of
// the chain), not here, to keep this function under the funlen limit.
func getRemainingServiceProviders() []service.Provider {
	return append([]service.Provider{
		&apigwbackend.Provider{},
		&cwlogsbackend.Provider{},
		&sfnbackend.Provider{},
		&cwbackend.Provider{},
		&kinesisbackend.Provider{},
		&elasticachebackend.Provider{},
		&route53backend.Provider{},
		&sesbackend.Provider{},
		&sesv2backend.Provider{},
		&ec2backend.Provider{},
		&opensearchbackend.Provider{},
		&acmbackend.Provider{},
		&acmpcabackend.Provider{},
		&redshiftbackend.Provider{},
		&redshiftbackend.ServerlessProvider{},
		&awsconfigbackend.Provider{},
		&s3controlbackend.Provider{},
		&resourcegroupsbackend.Provider{},
		&resourcegroupstaggingapibackend.Provider{},
		&swfbackend.Provider{},
		&firehosebackend.Provider{},
		&networkmonitorbackend.Provider{},
		&schedulerbackend.Provider{},
		&route53resolverbackend.Provider{},
		&rdsbackend.Provider{},
		&transcribebackend.Provider{},
		&pollybackend.Provider{},
		&supportbackend.Provider{},
		&ecrbackend.Provider{},
		&ecsbackend.Provider{},
		&fisbackend.Provider{},
		&identitystorebackend.Provider{},
		&organizationsbackend.Provider{},
		&cognitoidpbackend.Provider{},
		&cognitoidentitybackend.Provider{},
		&iotbackend.Provider{},
		&iotdataplanebackend.Provider{},
		&appsyncbackend.Provider{},
		&apigwmgmtbackend.Provider{},
		&appconfigdatabackend.Provider{},
		&amplifybackend.Provider{},
		&autoscalingbackend.Provider{},
		&apigwv2backend.Provider{},
		&athenabackend.Provider{},
		&appconfigbackend.Provider{},
		&backupbackend.Provider{},
		&cloudtrailbackend.Provider{},
		&applicationautoscalingbackend.Provider{},
		&batchbackend.Provider{},
		&bedrockbackend.Provider{},
		&bedrockbackend.AgentsProvider{},
		&bedrockruntimebackend.Provider{},
		&cebackend.Provider{},
		&cloudcontrolbackend.Provider{},
		&cloudfrontbackend.Provider{},
		&codeartifactbackend.Provider{},
		&codebuildbackend.Provider{},
		&codecommitbackend.Provider{},
		&codepipelinebackend.Provider{},
		&codeconnectionsbackend.Provider{},
		&codedeploybackend.Provider{},
		&dmsbackend.Provider{},
		&codestarconnectionsbackend.Provider{},
		&dynamodbstreamsbackend.Provider{},
		&elasticbeanstalkbackend.Provider{},
		&elasticsearchbackend.Provider{},
		&efsbackend.Provider{},
		&eksbackend.Provider{},
		&elbbackend.Provider{},
		&elbv2backend.Provider{},
		&emrserverlessbackend.Provider{},
		&emrbackend.Provider{},
		&gluebackend.Provider{},
		&guarddutybackend.Provider{},
		&inspector2backend.Provider{},
		&docdbbackend.Provider{},
		&glacierbackend.Provider{},
		&iotanalyticsbackend.Provider{},
		&iotwirelessbackend.Provider{},
		&kinesisanalyticsbackend.Provider{},
		&kafkabackend.Provider{},
		&kinesisanalyticsv2backend.Provider{},
		&lakeformationbackend.Provider{},
		&managedblockchainbackend.Provider{},
		&mediaconvertbackend.Provider{},
		&mqbackend.Provider{},
		&mediastorebackend.Provider{},
	}, getLatestServiceProviders()...)
}

// getLatestServiceProviders returns providers for additional services.
// Extracted from getServiceProviders to satisfy the funlen limit.
func getLatestServiceProviders() []service.Provider {
	return append([]service.Provider{
		&mediastoredatabackend.Provider{},
		&memorydbbackend.Provider{},
	}, getNewestServiceProviders()...)
}

// getNewestServiceProviders returns the most recently added service providers.
// Extracted from getServiceProviders to satisfy the funlen limit.
func getNewestServiceProviders() []service.Provider {
	return append([]service.Provider{
		&mwaabackend.Provider{},
		&neptunebackend.Provider{},
	}, getMostRecentServiceProviders()...)
}

func getMostRecentServiceProviders() []service.Provider {
	return []service.Provider{
		&pinpointbackend.Provider{},
		&pipesbackend.Provider{},
		&accessanalyzerbackend.Provider{},
		&rambackend.Provider{},
		&rolesanywherebackend.Provider{},
		&rdsdatabackend.Provider{},
		&redshiftdatabackend.Provider{},
		&sagemakerbackend.Provider{},
		&sagemakerruntimebackend.Provider{},
		&servicediscoverybackend.Provider{},
		&serverlessrepobackend.Provider{},
		&shieldbackend.Provider{},
		&ssoadminbackend.Provider{},
		&textractbackend.Provider{},
		&comprehendbackend.Provider{},
		&timestreamwritebackend.Provider{},
		&timestreamquerybackend.Provider{},
		&transferbackend.Provider{},
		&verifiedpermissionsbackend.Provider{},
		&wafbackend.Provider{},
		&wafv2backend.Provider{},
		&workmailbackend.Provider{},
		&workspacesbackend.Provider{},
		&xraybackend.Provider{},
		&s3tablesbackend.Provider{},
		&databrewbackend.Provider{},
		&cleanroomsbackend.Provider{},
		&directoryservicebackend.Provider{},
		&forecastbackend.Provider{},
		&mediatailorbackend.Provider{},
		&macie2backend.Provider{},
		&apprunnerbackend.Provider{},
		&appmeshbackend.Provider{},
		&appstreambackend.Provider{},
		&detectivebackend.Provider{},
		&datasyncbackend.Provider{},
		&dlmbackend.Provider{},
		&fsxbackend.Provider{},
		&daxbackend.Provider{},
		&medialivebackend.Provider{},
		&mediapackagebackend.Provider{},
		&personalizebackend.Provider{},
		&quicksightbackend.Provider{},
		&rekognitionbackend.Provider{},
		&translatebackend.Provider{},
		&securityhubbackend.Provider{},
		&vpclatticebackend.Provider{},
		&omicsbackend.Provider{},
		&bedrockagentbackend.Provider{},
	}
}

// startPurgeWorker runs the auto-purge ticker loop.
// It calls Purge on every service.Purgeable with a per-service timeout context
// so a slow or deadlocked backend cannot stall the entire purge cycle.
// startPurgeWorker periodically checks for and deletes resources older than the configured TTL.
// It dynamically reads the TTL from the global configuration, allowing runtime updates.
func startPurgeWorker(
	ctx context.Context,
	log *slog.Logger,
	gcfg *config.GlobalConfig,
	svcs []service.Registerable,
) {
	const (
		purgeTimeout  = 30 * time.Second
		checkInterval = 10 * time.Second
	)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	var lastPurgeAt time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttl := gcfg.GetAutoPurgeTTL()
			if ttl <= 0 {
				continue
			}

			// Avoid purging too frequently if the interval is short.
			// Only purge if at least 'ttl' has passed since the last start,
			// or if we've never purged.
			if !lastPurgeAt.IsZero() && time.Since(lastPurgeAt) < ttl {
				continue
			}

			lastPurgeAt = time.Now().UTC()
			cutoff := lastPurgeAt.Add(-ttl)
			log.InfoContext(ctx, "running automatic service purge", "ttl", ttl, "cutoff", cutoff)
			purgeAllServices(ctx, log, svcs, cutoff, purgeTimeout)
		}
	}
}

// purgeAllServices calls Purge on each Purgeable service with a timeout context.
func purgeAllServices(
	ctx context.Context,
	log *slog.Logger,
	svcs []service.Registerable,
	cutoff time.Time,
	timeout time.Duration,
) {
	const goroutineGracePeriod = 5 * time.Second

	for _, svc := range svcs {
		p, ok := svc.(service.Purgeable)
		if !ok {
			continue
		}
		purgeCtx, cancel := context.WithTimeout(ctx, timeout)
		done := make(chan struct{})
		go func() {
			defer close(done)
			p.Purge(purgeCtx, cutoff)
		}()
		select {
		case <-done:
			cancel()
		case <-purgeCtx.Done():
			cancel()
			if ctx.Err() == nil {
				log.WarnContext(ctx, "purge timed out", "service", svc.Name(), "timeout", timeout)
			}
			// Drain the goroutine with a grace period so it does not accumulate
			// across repeated purge cycles.
			graceTimer := time.NewTimer(goroutineGracePeriod)
			select {
			case <-done:
				graceTimer.Stop()
			case <-graceTimer.C:
				log.WarnContext(ctx, "purge goroutine did not exit after grace period",
					"service", svc.Name())
			}
		}
	}
}

// startBackgroundWorkers starts all background workers from services.
func startBackgroundWorkers(ctx context.Context, services []service.Registerable) {
	log := logger.Load(ctx)

	for _, svc := range services {
		if worker, ok := svc.(service.BackgroundWorker); ok {
			if workerErr := worker.StartWorker(ctx); workerErr != nil {
				log.ErrorContext(ctx, "failed to start background worker", "error", workerErr)
			}
		}
	}
}

// shutdownServices calls Shutdown on every service that implements service.Shutdowner.
// All shutdowns run concurrently. shutdownServices blocks until all complete or ctx
// expires (whichever comes first), logging a warning if the deadline is exceeded.
func shutdownServices(ctx context.Context, services []service.Registerable) {
	log := logger.Load(ctx)

	var wg sync.WaitGroup

	for _, svc := range services {
		if s, ok := svc.(service.Shutdowner); ok {
			wg.Go(func() {
				log.InfoContext(ctx, "shutting down service", "service", svc.Name())
				s.Shutdown(ctx)
			})
		}
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.WarnContext(
			ctx,
			"service shutdown timed out; some background goroutines may still be running",
		)
	}
}

// wireSNSToSQS connects the SNS publish emitter to the SQS delivery handler so
// that messages published to SNS topics are delivered to subscribed SQS queues.
// snsReg and sqsReg must be the service.Registerable values returned by their
// respective providers (indices 5 and 6 in the services slice).
func wireSNSToSQS(snsReg, sqsReg service.Registerable) {
	snsH, ok1 := snsReg.(*snsbackend.Handler)
	sqsH, ok2 := sqsReg.(*sqsbackend.Handler)

	if !ok1 || !ok2 {
		return
	}

	snsBk, ok3 := snsH.Backend.(*snsbackend.InMemoryBackend)
	sqsBk, ok4 := sqsH.Backend.(*sqsbackend.InMemoryBackend)

	if !ok3 || !ok4 {
		return
	}

	emitter := snsevents.NewInMemoryEmitter[*snsevents.SNSPublishedEvent]()
	snsBk.SetPublishEmitter(emitter)
	sqsBk.SubscribeToSNS(emitter)
}

// wireSQSMetrics wires the CloudWatch metric emitter into the SQS backend so that
// SendMessage, ReceiveMessage, and DeleteMessage operations emit CloudWatch metrics.
func wireSQSMetrics(sqsReg, cwReg service.Registerable) {
	sqsH, ok1 := sqsReg.(*sqsbackend.Handler)
	cwH, ok2 := cwReg.(*cwbackend.Handler)

	if !ok1 || !ok2 {
		return
	}

	sqsBk, bk1Ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	cwBk, bk2Ok := cwH.Backend.(*cwbackend.InMemoryBackend)

	if !bk1Ok || !bk2Ok {
		return
	}

	sqsBk.SetMetricEmitter(
		sqsbackend.MetricEmitterFunc(
			func(namespace, name string, value float64, unit string) error {
				_, err := cwBk.PutMetricData(namespace, []cwbackend.MetricDatum{
					{
						MetricName: name,
						Value:      value,
						Unit:       unit,
						Timestamp:  time.Now(),
					},
				})

				return err
			},
		),
	)
}

// wireEventBridgeDelivery connects EventBridge fan-out to Lambda, SQS, and SNS backends.
// ebReg, lambdaReg, sqsReg, snsReg must be the service.Registerable values returned
// by their respective providers (indices 10, 9, 6, 5 in the services slice).
func wireEventBridgeDelivery(ebReg, lambdaReg, sqsReg, snsReg service.Registerable) {
	ebH, ok := ebReg.(*ebbackend.Handler)
	if !ok {
		return
	}

	ebBk, bkOk := ebH.Backend.(*ebbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	dt := &ebbackend.DeliveryTargets{}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			dt.Lambda = lambdaBk
		}
	}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bk2Ok := sqsH.Backend.(*sqsbackend.InMemoryBackend); bk2Ok {
			dt.SQS = &sqsSenderAdapter{backend: sqsBk}
		}
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		if snsBk, bk2Ok := snsH.Backend.(*snsbackend.InMemoryBackend); bk2Ok {
			dt.SNS = &snsPublisherAdapter{backend: snsBk}
		}
	}

	ebBk.SetDeliveryTargets(dt)
}

// sqsSenderAdapter adapts the SQS backend to the eventbridge.SQSSender interface.
type sqsSenderAdapter struct {
	backend *sqsbackend.InMemoryBackend
}

func (a *sqsSenderAdapter) SendMessageToQueue(
	_ context.Context,
	queueARN, messageBody string,
) error {
	// Convert SQS ARN to queue name (last segment after ':').
	queueURL := arnToSQSQueueURL(queueARN)
	_, err := a.backend.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL:    queueURL,
		MessageBody: messageBody,
	})

	return err
}

// snsPublisherAdapter adapts the SNS backend to the eventbridge.SNSPublisher interface.
type snsPublisherAdapter struct {
	backend *snsbackend.InMemoryBackend
}

func (a *snsPublisherAdapter) PublishToTopic(_ context.Context, topicARN, message string) error {
	_, err := a.backend.Publish(topicARN, message, "", "", nil)

	return err
}

// wireS3Notifications connects the S3 handler to SQS, SNS, Lambda, and EventBridge backends so that
// bucket notification configurations are honoured on PutObject, CopyObject, DeleteObject, and CompleteMultipartUpload.
func wireS3Notifications(s3Reg, sqsReg, snsReg, lambdaReg, ebReg service.Registerable) {
	s3H, ok := s3Reg.(*s3backend.S3Handler)
	if !ok {
		return
	}

	targets := &s3backend.NotificationTargets{}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bkOk := sqsH.Backend.(*sqsbackend.InMemoryBackend); bkOk {
			targets.SQSSender = &sqsSenderAdapter{backend: sqsBk}
		}
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		if snsBk, bkOk := snsH.Backend.(*snsbackend.InMemoryBackend); bkOk {
			targets.SNSPublisher = &s3SNSPublisherAdapter{backend: snsBk}
		}
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			targets.LambdaInvoker = lambdaBk
		}
	}

	if ebH, ebOk := ebReg.(*ebbackend.Handler); ebOk {
		if ebBk, bkOk := ebH.Backend.(*ebbackend.InMemoryBackend); bkOk {
			targets.EventBridgePublisher = &s3EventBridgeAdapter{backend: ebBk}
		}
	}

	s3H.SetNotificationDispatcher(
		s3backend.NewNotificationDispatcher(targets, config.DefaultRegion),
	)
}

// s3SNSPublisherAdapter adapts the SNS backend to the s3.SNSPublisher interface.
type s3SNSPublisherAdapter struct {
	backend *snsbackend.InMemoryBackend
}

func (a *s3SNSPublisherAdapter) PublishToTopic(
	_ context.Context,
	topicARN, message, _ string,
) error {
	_, err := a.backend.Publish(topicARN, message, "", "", nil)

	return err
}

// s3EventBridgeAdapter adapts the EventBridge backend to the s3.EventBridgePublisher interface.
type s3EventBridgeAdapter struct {
	backend *ebbackend.InMemoryBackend
}

func (a *s3EventBridgeAdapter) PublishS3Event(
	ctx context.Context,
	source, detailType, detail string,
) {
	a.backend.PutEvents(ctx, []ebbackend.EventEntry{
		{Source: source, DetailType: detailType, Detail: detail},
	})
}

// wireSSMKMS connects the SSM backend to the KMS backend so that SecureString
// parameters whose KeyId is set are encrypted/decrypted using real KMS keys.
func wireSSMKMS(ssmReg, kmsReg service.Registerable) {
	ssmH, ok := ssmReg.(*ssmbackend.Handler)
	if !ok {
		return
	}
	ssmBk, ok := ssmH.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return
	}
	kmsH, ok := kmsReg.(*kmsbackend.Handler)
	if !ok {
		return
	}
	kmsBk, ok := kmsH.Backend.(*kmsbackend.InMemoryBackend)
	if !ok {
		return
	}
	ssmBk.WithKMS(&ssmKMSAdapter{backend: kmsBk})
}

// ssmKMSAdapter adapts kms.InMemoryBackend to ssm.KMSEncryptor.
type ssmKMSAdapter struct {
	backend *kmsbackend.InMemoryBackend
}

func (a *ssmKMSAdapter) EncryptSSM(keyID string, plaintext []byte) ([]byte, error) {
	out, err := a.backend.Encrypt(context.Background(), &kmsbackend.EncryptInput{
		KeyID:     keyID,
		Plaintext: plaintext,
	})
	if err != nil {
		return nil, err
	}

	return out.CiphertextBlob, nil
}

func (a *ssmKMSAdapter) DecryptSSM(ciphertext []byte) ([]byte, error) {
	out, err := a.backend.Decrypt(context.Background(), &kmsbackend.DecryptInput{
		CiphertextBlob: ciphertext,
	})
	if err != nil {
		return nil, err
	}

	return out.Plaintext, nil
}

// wireAPIGatewayLambda connects the API Gateway handler to the Lambda backend
// for AWS_PROXY integrations.
func wireAPIGatewayLambda(apigwReg, lambdaReg service.Registerable) {
	apigwH, ok := apigwReg.(*apigwbackend.Handler)
	if !ok {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			apigwH.SetLambdaInvoker(lambdaBk)
		}
	}
}

// so that Task states with Lambda resources can invoke functions.
func wireStepFunctionsLambda(sfnReg, lambdaReg service.Registerable) {
	sfnH, ok := sfnReg.(*sfnbackend.Handler)
	if !ok {
		return
	}

	sfnBk, bkOk := sfnH.Backend.(*sfnbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			sfnBk.SetLambdaInvoker(lambdaBk)
		}
	}
}

// wireStepFunctionsServiceIntegrations connects the Step Functions backend to SQS, SNS, and DynamoDB backends
// so that Task states with service integration resources can invoke those services.
func wireStepFunctionsServiceIntegrations(sfnReg, sqsReg, snsReg, ddbReg service.Registerable) {
	sfnH, ok := sfnReg.(*sfnbackend.Handler)
	if !ok {
		return
	}

	sfnBk, bkOk := sfnH.Backend.(*sfnbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		sfnBk.SetSQSIntegration(sfnbackend.NewSQSIntegration(sqsH.Backend))
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		sfnBk.SetSNSIntegration(sfnbackend.NewSNSIntegration(snsH.Backend))
	}

	if ddbH, ddbOk := ddbReg.(*ddbbackend.DynamoDBHandler); ddbOk {
		sfnBk.SetDynamoDBIntegration(sfnbackend.NewDynamoDBIntegration(ddbH.Backend))
	}
}

// wireKinesisLambda connects the Kinesis backend to the Lambda event source poller
// so that records written to Kinesis streams trigger Lambda functions with active
// event source mappings.
func wireKinesisLambda(kinesisReg, lambdaReg service.Registerable) {
	kinesisH, ok := kinesisReg.(*kinesisbackend.Handler)
	if !ok {
		return
	}

	kinesisBk, bkOk := kinesisH.Backend.(*kinesisbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			adapter := &kinesisReaderAdapter{backend: kinesisBk}
			lambdaBk.SetKinesisPoller(lambdabackend.NewEventSourcePoller(lambdaBk, adapter))
		}
	}
}

// kinesisReaderAdapter adapts the Kinesis backend to the lambda.KinesisReader interface.
type kinesisReaderAdapter struct {
	backend *kinesisbackend.InMemoryBackend
}

func (a *kinesisReaderAdapter) GetShardIDs(streamName string) ([]string, error) {
	out, err := a.backend.DescribeStream(
		context.Background(),
		&kinesisbackend.DescribeStreamInput{StreamName: streamName},
	)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(out.Shards))
	for i, s := range out.Shards {
		ids[i] = s.ShardID
	}

	return ids, nil
}

func (a *kinesisReaderAdapter) GetShardIterator(
	streamName, shardID, iteratorType, startingSeqNum string,
) (string, error) {
	out, err := a.backend.GetShardIterator(context.Background(), &kinesisbackend.GetShardIteratorInput{
		StreamName:             streamName,
		ShardID:                shardID,
		ShardIteratorType:      iteratorType,
		StartingSequenceNumber: startingSeqNum,
	})
	if err != nil {
		return "", err
	}

	return out.ShardIterator, nil
}

func (a *kinesisReaderAdapter) GetRecords(
	iteratorToken string,
	limit int,
) ([]lambdabackend.KinesisRecord, string, error) {
	out, err := a.backend.GetRecords(context.Background(), &kinesisbackend.GetRecordsInput{
		ShardIterator: iteratorToken,
		Limit:         limit,
	})
	if err != nil {
		return nil, "", err
	}

	records := make([]lambdabackend.KinesisRecord, len(out.Records))
	for i, r := range out.Records {
		records[i] = lambdabackend.KinesisRecord{
			PartitionKey:   r.PartitionKey,
			SequenceNumber: r.SequenceNumber,
			Data:           r.Data,
			ArrivalTime:    r.ApproximateArrivalTimestamp,
		}
	}

	return records, out.NextShardIterator, nil
}

// ARN format: arn:aws:sqs:region:accountId:queueName
// URL format expected by SQS backend: http://endpoint/accountId/queueName
func arnToSQSQueueURL(arn string) string {
	parts := strings.Split(arn, ":")
	// Minimum parts for a valid SQS ARN: arn, aws, sqs, region, accountId, queueName
	const minARNParts = 6
	if len(parts) < minARNParts {
		return arn
	}

	accountID := parts[4]
	queueName := parts[5]

	return "http://local/" + accountID + "/" + queueName
}

// wireSQSLambda connects the SQS backend to the Lambda event source poller so
// that messages enqueued in SQS queues trigger Lambda functions with active
// SQS event source mappings.
func wireSQSLambda(sqsReg, lambdaReg service.Registerable) {
	sqsH, ok := sqsReg.(*sqsbackend.Handler)
	if !ok {
		return
	}

	sqsBk, bkOk := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			lambdaBk.SetSQSReader(&sqsReaderAdapter{backend: sqsBk})
		}
	}
}

// sqsReaderAdapter adapts the SQS InMemoryBackend to the lambda.SQSReader interface.
type sqsReaderAdapter struct {
	backend *sqsbackend.InMemoryBackend
}

func (a *sqsReaderAdapter) ReceiveMessagesLocal(
	queueARN string,
	maxMessages int,
) ([]*lambdabackend.SQSMessage, error) {
	url := arnToSQSQueueURL(queueARN)

	msgs, err := a.backend.ReceiveMessagesLocal(url, maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*lambdabackend.SQSMessage, len(msgs))
	for i, m := range msgs {
		result[i] = &lambdabackend.SQSMessage{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			Attributes:    m.Attributes,
			MD5OfBody:     m.MD5OfBody,
		}
	}

	return result, nil
}

func (a *sqsReaderAdapter) DeleteMessagesLocal(queueARN string, receiptHandles []string) error {
	url := arnToSQSQueueURL(queueARN)

	return a.backend.DeleteMessagesLocal(url, receiptHandles)
}

// wireDynamoDBStreamLambda connects the DynamoDB Streams backend to the Lambda event source
// poller so that stream records trigger Lambda functions with active DynamoDB ESMs.
func wireDynamoDBStreamLambda(ddbReg, lambdaReg service.Registerable) {
	ddbH, ok := ddbReg.(*ddbbackend.DynamoDBHandler)
	if !ok {
		return
	}

	ddbBk, bkOk := ddbH.Backend.(*ddbbackend.InMemoryDB)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			lambdaBk.SetDynamoDBStreamsReader(&ddbStreamsReaderAdapter{backend: ddbBk})
		}
	}
}

// ddbStreamsReaderAdapter adapts the DynamoDB InMemoryDB to the lambda.DynamoDBStreamsReader interface.
type ddbStreamsReaderAdapter struct {
	backend *ddbbackend.InMemoryDB
}

func (a *ddbStreamsReaderAdapter) DescribeStreamShards(streamARN string) ([]string, error) {
	out, err := a.backend.DescribeStream(
		context.Background(),
		&awsddbstreams.DescribeStreamInput{StreamArn: aws.String(streamARN)},
	)
	if err != nil {
		return nil, err
	}

	if out.StreamDescription == nil {
		return nil, nil
	}

	shardIDs := make([]string, 0, len(out.StreamDescription.Shards))
	for _, s := range out.StreamDescription.Shards {
		if s.ShardId != nil {
			shardIDs = append(shardIDs, *s.ShardId)
		}
	}

	return shardIDs, nil
}

func (a *ddbStreamsReaderAdapter) GetStreamShardIterator(
	streamARN, shardID, iteratorType string,
) (string, error) {
	out, err := a.backend.GetShardIterator(
		context.Background(),
		&awsddbstreams.GetShardIteratorInput{
			StreamArn:         aws.String(streamARN),
			ShardId:           aws.String(shardID),
			ShardIteratorType: ddbstreamstypes.ShardIteratorType(iteratorType),
		},
	)
	if err != nil {
		return "", err
	}

	return aws.ToString(out.ShardIterator), nil
}

func (a *ddbStreamsReaderAdapter) GetStreamRecords(
	iteratorToken string,
	limit int,
) ([]lambdabackend.DynamoDBStreamRecord, string, error) {
	// Clamp limit to a valid int32 range. BatchSize is bounded by the Lambda ESM (max 10 000),
	// but we guard defensively against any caller passing a value outside int32 range.
	const maxStreamRecordsLimit = math.MaxInt32
	if limit <= 0 || limit > maxStreamRecordsLimit {
		limit = maxStreamRecordsLimit
	}

	lim := int32(math.MaxInt32)
	if limit > 0 && limit <= maxStreamRecordsLimit {
		lim = int32(limit)
	}
	out, err := a.backend.GetRecords(context.Background(), &awsddbstreams.GetRecordsInput{
		ShardIterator: aws.String(iteratorToken),
		Limit:         &lim,
	})
	if err != nil {
		return nil, "", err
	}

	records := make([]lambdabackend.DynamoDBStreamRecord, 0, len(out.Records))

	for _, r := range out.Records {
		rec := lambdabackend.DynamoDBStreamRecord{
			EventID:   aws.ToString(r.EventID),
			EventName: string(r.EventName),
		}

		populateDDBStreamRecord(&rec, r.Dynamodb)
		records = append(records, rec)
	}

	return records, aws.ToString(out.NextShardIterator), nil
}

// populateDDBStreamRecord fills in the DynamoDB-specific fields of a DynamoDBStreamRecord
// from the SDK StreamRecord payload.
func populateDDBStreamRecord(
	rec *lambdabackend.DynamoDBStreamRecord,
	ddb *ddbstreamstypes.StreamRecord,
) {
	if ddb == nil {
		return
	}

	rec.SequenceNumber = aws.ToString(ddb.SequenceNumber)
	rec.StreamViewType = string(ddb.StreamViewType)

	if ddb.SizeBytes != nil {
		rec.SizeBytes = *ddb.SizeBytes
	}

	if ddb.ApproximateCreationDateTime != nil {
		rec.ApproximateCreationDateTime = float64(ddb.ApproximateCreationDateTime.Unix())
	}

	if ddb.NewImage != nil {
		rec.NewImage = sdkDDBStreamItemToWire(ddb.NewImage)
	}

	if ddb.OldImage != nil {
		rec.OldImage = sdkDDBStreamItemToWire(ddb.OldImage)
	}

	if ddb.Keys != nil {
		rec.Keys = sdkDDBStreamItemToWire(ddb.Keys)
	}
}

// sdkDDBStreamItemToWire converts a DynamoDB Streams SDK attribute map to DynamoDB JSON wire format.
func sdkDDBStreamItemToWire(item map[string]ddbstreamstypes.AttributeValue) map[string]any {
	out := make(map[string]any, len(item))

	for k, v := range item {
		if w := sdkDDBStreamAttrToWire(v); w != nil {
			out[k] = w
		}
	}

	return out
}

// sdkDDBStreamAttrToWire converts a single DynamoDB Streams SDK attribute value to DynamoDB JSON
// wire format (map[string]any). Unknown attribute types are returned as nil and are excluded by
// the caller.
func sdkDDBStreamAttrToWire(av ddbstreamstypes.AttributeValue) map[string]any {
	switch v := av.(type) {
	case *ddbstreamstypes.AttributeValueMemberS:
		return map[string]any{"S": v.Value}
	case *ddbstreamstypes.AttributeValueMemberN:
		return map[string]any{"N": v.Value}
	case *ddbstreamstypes.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": v.Value}
	case *ddbstreamstypes.AttributeValueMemberNULL:
		return map[string]any{"NULL": v.Value}
	case *ddbstreamstypes.AttributeValueMemberB:
		return map[string]any{"B": v.Value}
	case *ddbstreamstypes.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}
	case *ddbstreamstypes.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}
	case *ddbstreamstypes.AttributeValueMemberBS:
		return map[string]any{"BS": v.Value}
	case *ddbstreamstypes.AttributeValueMemberM:
		return map[string]any{"M": sdkDDBStreamItemToWire(v.Value)}
	case *ddbstreamstypes.AttributeValueMemberL:
		items := make([]any, len(v.Value))
		for i, elem := range v.Value {
			items[i] = sdkDDBStreamAttrToWire(elem)
		}

		return map[string]any{"L": items}
	}

	return nil
}

// wireCloudWatchAlarmActions connects the CloudWatch backend to SNS and Lambda so that
// alarm state transitions trigger action notifications.
func wireCloudWatchAlarmActions(cwReg, snsReg, lambdaReg service.Registerable) {
	cwH, ok1 := cwReg.(*cwbackend.Handler)
	snsH, ok2 := snsReg.(*snsbackend.Handler)
	lambdaH, ok3 := lambdaReg.(*lambdabackend.Handler)

	if !ok1 {
		return
	}

	cwBk, ok4 := cwH.Backend.(*cwbackend.InMemoryBackend)
	if !ok4 {
		return
	}

	if ok2 {
		if snsBk, isSNS := snsH.Backend.(*snsbackend.InMemoryBackend); isSNS {
			cwBk.SetSNSPublisher(&cwSNSPublisherAdapter{backend: snsBk})
		}
	}

	if ok3 {
		if lambdaBk, isLambda := lambdaH.Backend.(*lambdabackend.InMemoryBackend); isLambda {
			cwBk.SetLambdaInvoker(&cwLambdaInvokerAdapter{backend: lambdaBk})
		}
	}
}

// cwSNSPublisherAdapter adapts the SNS backend to the cloudwatch.SNSPublisher interface.
type cwSNSPublisherAdapter struct {
	backend *snsbackend.InMemoryBackend
}

func (a *cwSNSPublisherAdapter) PublishToTopic(topicARN, message string) error {
	_, err := a.backend.Publish(topicARN, message, "CloudWatch Alarm", "", nil)

	return err
}

// cwLambdaInvokerAdapter adapts the Lambda backend to the cloudwatch.LambdaInvoker interface.
type cwLambdaInvokerAdapter struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *cwLambdaInvokerAdapter) InvokeFunction(
	ctx context.Context,
	name string,
	_ string,
	payload []byte,
) ([]byte, int, error) {
	return a.backend.InvokeFunction(ctx, name, lambdabackend.InvocationTypeEvent, payload)
}

// wireLambdaCWLogs connects the Lambda backend to CloudWatch Logs so that
// function invocations produce log entries in /aws/lambda/{function-name}.
func wireLambdaCWLogs(lambdaReg, cwlogsReg service.Registerable) {
	lambdaH, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if cwlogsH, cwlogsOk := cwlogsReg.(*cwlogsbackend.Handler); cwlogsOk {
		if cwlogsBk, cwBkOk := cwlogsH.Backend.(*cwlogsbackend.InMemoryBackend); cwBkOk {
			lambdaBk.SetCWLogsBackend(&cwLogsAdapter{backend: cwlogsBk})
		}
	}
}

// cwLogsAdapter adapts the CloudWatch Logs InMemoryBackend to the lambda.CWLogsBackend interface.
type cwLogsAdapter struct {
	backend *cwlogsbackend.InMemoryBackend
}

func (a *cwLogsAdapter) EnsureLogGroupAndStream(groupName, streamName string) error {
	if _, err := a.backend.CreateLogGroup(context.Background(), groupName, "", ""); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogGroupAlreadyExists) {
		return err
	}

	if _, err := a.backend.CreateLogStream(context.Background(), groupName, streamName); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return err
	}

	return nil
}

func (a *cwLogsAdapter) PutLogLines(groupName, streamName string, messages []string) error {
	events := make([]cwlogsbackend.InputLogEvent, len(messages))
	now := time.Now().UnixMilli()

	for i, msg := range messages {
		events[i] = cwlogsbackend.InputLogEvent{Message: msg, Timestamp: now}
	}

	_, err := a.backend.PutLogEvents(context.Background(), groupName, streamName, "", events)

	return err
}

// wireCWLogsSubscriptionFilters wires the CloudWatch Logs subscription filter delivery
// to Lambda, Kinesis, and Firehose backends.
func wireCWLogsSubscriptionFilters(
	cwlogsReg, lambdaReg, kinesisReg, firehoseReg service.Registerable,
) {
	cwlogsH, ok := cwlogsReg.(*cwlogsbackend.Handler)
	if !ok {
		return
	}

	cwlogsBk, bkOk := cwlogsH.Backend.(*cwlogsbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	d := &cwlogsSubscriptionDeliverer{}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			d.lambda = lambdaBk
		}
	}

	if kinesisH, kinesisOk := kinesisReg.(*kinesisbackend.Handler); kinesisOk {
		if kinesisBk, bk2Ok := kinesisH.Backend.(*kinesisbackend.InMemoryBackend); bk2Ok {
			d.kinesis = kinesisBk
		}
	}

	if firehoseH, firehoseOk := firehoseReg.(*firehosebackend.Handler); firehoseOk {
		if fhBk, fhBkOk := firehoseH.Backend.(*firehosebackend.InMemoryBackend); fhBkOk {
			d.firehose = fhBk
		} else {
			slog.Default().
				Warn("cwlogs: firehose backend is not *InMemoryBackend; subscription delivery to firehose disabled")
		}
	}

	cwlogsBk.SetSubscriptionDeliverer(d)
}

// cwlogsSubscriptionDeliverer delivers CloudWatch Logs subscription filter payloads to
// Lambda, Kinesis, and Firehose destinations by parsing the destination ARN.
type cwlogsSubscriptionDeliverer struct {
	lambda   *lambdabackend.InMemoryBackend
	kinesis  *kinesisbackend.InMemoryBackend
	firehose *firehosebackend.InMemoryBackend
}

func (d *cwlogsSubscriptionDeliverer) DeliverLogEvents(
	ctx context.Context, destinationArn string, payload []byte,
) error {
	// ARN format: arn:aws:<service>:<region>:<account>:<resource>
	const arnParts = 6
	parts := strings.SplitN(destinationArn, ":", arnParts)
	const arnServiceIdx = 2
	const arnResourceIdx = 5

	if len(parts) < arnParts {
		return nil
	}

	service := parts[arnServiceIdx]
	resource := parts[arnResourceIdx]

	switch service {
	case "lambda":
		if d.lambda == nil {
			return nil
		}
		// resource is "function:<name>" or just "<name>"
		funcName := strings.TrimPrefix(resource, "function:")
		_, _, err := d.lambda.InvokeFunction(
			ctx,
			funcName,
			lambdabackend.InvocationTypeEvent,
			payload,
		)

		return err
	case kinesisServiceName:
		if d.kinesis == nil {
			return nil
		}
		// resource is "stream/<name>"
		streamName := strings.TrimPrefix(resource, "stream/")
		_, err := d.kinesis.PutRecord(ctx, &kinesisbackend.PutRecordInput{
			StreamName:   streamName,
			PartitionKey: "cwlogs",
			Data:         payload,
		})

		return err
	case "firehose":
		if d.firehose == nil {
			return nil
		}
		// resource is "deliverystream/<name>"
		streamName := strings.TrimPrefix(resource, "deliverystream/")

		return d.firehose.PutRecord(ctx, streamName, payload)
	}

	return nil
}

// wireCWLogsMetricEmitter connects the CloudWatch Logs backend to the CloudWatch Metrics backend
// so that metric filter matches on PutLogEvents are forwarded as CloudWatch metric data points.
func wireCWLogsMetricEmitter(cwlogsReg, cwReg service.Registerable) {
	cwlogsH, ok := cwlogsReg.(*cwlogsbackend.Handler)
	if !ok {
		return
	}

	cwlogsBk, bkOk := cwlogsH.Backend.(*cwlogsbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	cwH, cwOk := cwReg.(*cwbackend.Handler)
	if !cwOk {
		return
	}

	cwBk, cwBkOk := cwH.Backend.(*cwbackend.InMemoryBackend)
	if !cwBkOk {
		return
	}

	cwlogsBk.SetMetricEmitter(
		cwlogsbackend.MetricEmitterFunc(
			func(namespace, name string, value float64, unit string) error {
				_, err := cwBk.PutMetricData(namespace, []cwbackend.MetricDatum{
					{
						MetricName: name,
						Namespace:  namespace,
						Value:      value,
						Unit:       unit,
						Timestamp:  time.Now(),
					},
				})

				return err
			},
		),
	)
}

// wireIAMToSTS connects the IAM backend to STS so that AssumeRole can validate
// ExternalId conditions and enforce per-role MaxSessionDuration limits.
func wireIAMToSTS(iamReg, stsReg service.Registerable) {
	iamH, iamOk := iamReg.(*iambackend.Handler)
	stsH, stsOk := stsReg.(*stsbackend.Handler)

	if !iamOk || !stsOk {
		return
	}

	iamBk, iamBkOk := iamH.Backend.(*iambackend.InMemoryBackend)
	stsBk, stsBkOk := stsH.Backend.(*stsbackend.InMemoryBackend)

	if !iamBkOk || !stsBkOk {
		return
	}

	stsBk.SetRoleLookup(&iamRoleLookupAdapter{backend: iamBk})
	stsBk.SetOIDCLookup(iamBk)
}

// iamRoleLookupAdapter adapts the IAM backend to the STS RoleLookup interface.
type iamRoleLookupAdapter struct {
	backend *iambackend.InMemoryBackend
}

// GetRoleByArn looks up the IAM role by ARN and returns STS-relevant metadata.
func (a *iamRoleLookupAdapter) GetRoleByArn(roleArn string) (*stsbackend.RoleMeta, error) {
	role, err := a.backend.GetRoleByArn(roleArn)
	if err != nil {
		return nil, err
	}

	return &stsbackend.RoleMeta{
		TrustPolicy:        role.AssumeRolePolicyDocument,
		MaxSessionDuration: role.MaxSessionDuration,
	}, nil
}

// wireSecretsManagerLambda wires the Lambda invoker into the SecretsManager handler
// so that RotateSecret with a RotationLambdaARN invokes the Lambda function.
func wireSecretsManagerLambda(smReg, lambdaReg service.Registerable) {
	smH, ok := smReg.(*secretsmanagerbackend.Handler)
	if !ok {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			smH.SetLambdaInvoker(lambdaBk)
		}
	}
}

// wireIoTRules connects the IoT rule dispatcher to SQS and Lambda backends, and
// wires the IoT MQTT broker into the IoT Data Plane backend.
func wireIoTRules(iotReg, iotDPReg, sqsReg, lambdaReg service.Registerable) {
	iotH, ok := iotReg.(*iotbackend.Handler)
	if !ok {
		return
	}

	iotBk, bkOk := iotH.Backend.(*iotbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	var sqsBk *sqsbackend.InMemoryBackend
	var lambdaBk *lambdabackend.InMemoryBackend

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		sqsBk, _ = sqsH.Backend.(*sqsbackend.InMemoryBackend)
	}

	if lambdaH, lamOk := lambdaReg.(*lambdabackend.Handler); lamOk {
		lambdaBk, _ = lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	}

	iotBk.SetRuleDispatcher(&iotRuleDispatcher{sqs: sqsBk, lambda: lambdaBk})

	// Wire the MQTT broker into the IoT Data Plane backend.
	if iotDPReg != nil {
		if dpH, dpOk := iotDPReg.(*iotdataplanebackend.Handler); dpOk {
			if dpBk, dpBkOk := dpH.Backend.(*iotdataplanebackend.InMemoryBackend); dpBkOk {
				dpBk.SetBroker(iotH.Broker())
			}
		}
	}
}

// wireAppSyncLambda connects the AppSync backend to the Lambda backend so that
// LAMBDA data source resolvers can invoke Lambda functions.
func wireAppSyncLambda(appSyncReg, lambdaReg service.Registerable) {
	appSyncH, ok := appSyncReg.(*appsyncbackend.Handler)
	if !ok {
		return
	}

	appSyncBk, bkOk := appSyncH.Backend.(*appsyncbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			appSyncBk.SetLambdaInvoker(lambdaBk)
		}
	}
}

// iotRuleDispatcher adapts the SQS and Lambda backends to the IoT RuleDispatcher interface.
type iotRuleDispatcher struct {
	sqs    *sqsbackend.InMemoryBackend
	lambda *lambdabackend.InMemoryBackend
}

func (d *iotRuleDispatcher) SendToSQS(queueURL, body string) error {
	if d.sqs == nil {
		return nil
	}

	_, err := d.sqs.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL:    queueURL,
		MessageBody: body,
	})

	return err
}

// wireAppSyncDynamoDB connects the AppSync backend to the DynamoDB backend so that
// AMAZON_DYNAMODB data source resolvers can perform GetItem/PutItem operations.
func wireAppSyncDynamoDB(appSyncReg, ddbReg service.Registerable) {
	appSyncH, ok := appSyncReg.(*appsyncbackend.Handler)
	if !ok {
		return
	}

	appSyncBk, bkOk := appSyncH.Backend.(*appsyncbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	if ddbH, ddbOk := ddbReg.(*ddbbackend.DynamoDBHandler); ddbOk {
		if ddbBk, bk3Ok := ddbH.Backend.(*ddbbackend.InMemoryDB); bk3Ok {
			appSyncBk.SetDynamoDBBackend(&dynamoDBAdapter{db: ddbBk})
		}
	}
}

// dynamoDBAdapter adapts ddbbackend.InMemoryDB to the appsync.DynamoDBBackend interface
// by converting between the wire (map[string]any) format and the SDK AttributeValue format.
type dynamoDBAdapter struct {
	db *ddbbackend.InMemoryDB
}

func (a *dynamoDBAdapter) GetItemRaw(
	ctx context.Context,
	tableName string,
	key map[string]any,
) (map[string]any, error) {
	sdkKey, err := ddbmodels.ToSDKItem(key)
	if err != nil {
		return nil, fmt.Errorf("appsync ddb adapter: marshal key: %w", err)
	}

	out, err := a.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &tableName,
		Key:       sdkKey,
	})
	if err != nil {
		return nil, err
	}

	if len(out.Item) == 0 {
		return map[string]any{}, nil
	}

	return ddbmodels.FromSDKItem(out.Item), nil
}

func (a *dynamoDBAdapter) PutItemRaw(
	ctx context.Context,
	tableName string,
	item map[string]any,
) error {
	sdkItem, err := ddbmodels.ToSDKItem(item)
	if err != nil {
		return fmt.Errorf("appsync ddb adapter: marshal item: %w", err)
	}

	_, err = a.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      sdkItem,
	})

	return err
}

func (d *iotRuleDispatcher) InvokeLambda(
	ctx context.Context,
	functionARN string,
	payload []byte,
) error {
	if d.lambda == nil {
		return nil
	}

	_, _, err := d.lambda.InvokeFunction(
		ctx,
		functionARN,
		lambdabackend.InvocationTypeEvent,
		payload,
	)

	return err
}

// arnServiceIs returns true if the ARN's service segment (the third colon-delimited field)
// matches the given service name exactly. This is more precise than a substring search since
// ARN format is "arn:aws:SERVICE:REGION:ACCOUNT:RESOURCE".
func arnServiceIs(a, serviceName string) bool {
	// Fast path: ARN must start with "arn:aws:" (or "arn:aws-cn:", "arn:aws-us-gov:", etc.)
	// We split on ":" up to 3 parts to extract just the service field.
	start := strings.Index(a, ":")
	if start < 0 {
		return false
	}

	start++ // skip past first ":"

	next := strings.Index(a[start:], ":")
	if next < 0 {
		return false
	}

	start += next + 1 // skip past second ":"

	end := strings.Index(a[start:], ":")
	if end < 0 {
		return false
	}

	return a[start:start+end] == serviceName
}

// registerTaggingService wires a single service's provider, ARN tagger, and ARN untagger into
// the Resource Groups Tagging API backend. arnService is the AWS service name used to match
// the service segment of an ARN (e.g., "sqs", "sns", "lambda").
func registerTaggingService(
	bk resourcegroupstaggingapibackend.StorageBackend,
	provider resourcegroupstaggingapibackend.ResourceProvider,
	arnService string,
	tagger func(string, map[string]string) error,
	untagger func(string, []string) error,
) {
	bk.RegisterProvider(provider)
	bk.RegisterARNTagger(func(_ context.Context, arn string, newTags map[string]string) (bool, error) {
		if !arnServiceIs(arn, arnService) {
			return false, nil
		}

		return true, tagger(arn, newTags)
	})
	bk.RegisterARNUntagger(func(_ context.Context, arn string, keys []string) (bool, error) {
		if !arnServiceIs(arn, arnService) {
			return false, nil
		}

		return true, untagger(arn, keys)
	})
}

// wireResourceGroupsTagging connects the Resource Groups Tagging API backend to all
// service backends so that GetResources, GetTagKeys, GetTagValues, TagResources, and
// UntagResources work cross-service.
func wireResourceGroupsTagging(
	taggingReg service.Registerable,
	ddbReg service.Registerable,
	sqsReg service.Registerable,
	snsReg service.Registerable,
	lambdaReg service.Registerable,
	kmsReg service.Registerable,
	smReg service.Registerable,
) {
	taggingH, ok := taggingReg.(*resourcegroupstaggingapibackend.Handler)
	if !ok {
		return
	}

	bk := taggingH.Backend

	wireTaggingDDB(bk, ddbReg)
	wireTaggingSQS(bk, sqsReg)
	wireTaggingSNS(bk, snsReg)
	wireTaggingLambda(bk, lambdaReg)
	wireTaggingKMS(bk, kmsReg)
	wireTaggingSM(bk, smReg)
}

func wireTaggingDDB(
	bk resourcegroupstaggingapibackend.StorageBackend,
	ddbReg service.Registerable,
) {
	ddbH, ok := ddbReg.(*ddbbackend.DynamoDBHandler)
	if !ok {
		return
	}

	ddbBk, ok := ddbH.Backend.(*ddbbackend.InMemoryDB)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			tables := ddbBk.TaggedTables()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(tables))
			for _, t := range tables {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  t.ARN,
					ResourceType: "dynamodb:table",
					Tags:         t.Tags,
				})
			}

			return out
		},
		"dynamodb",
		func(arn string, newTags map[string]string) error {
			sdkTags := make([]ddbsdktypes.Tag, 0, len(newTags))
			for k, v := range newTags {
				tagKey, tagValue := k, v
				sdkTags = append(sdkTags, ddbsdktypes.Tag{Key: &tagKey, Value: &tagValue})
			}

			_, err := ddbBk.TagResource(context.Background(), &dynamodb.TagResourceInput{
				ResourceArn: aws.String(arn),
				Tags:        sdkTags,
			})

			return err
		},
		func(arn string, keys []string) error {
			_, err := ddbBk.UntagResource(context.Background(), &dynamodb.UntagResourceInput{
				ResourceArn: aws.String(arn),
				TagKeys:     keys,
			})

			return err
		},
	)
}

func wireTaggingSQS(
	bk resourcegroupstaggingapibackend.StorageBackend,
	sqsReg service.Registerable,
) {
	sqsH, ok := sqsReg.(*sqsbackend.Handler)
	if !ok {
		return
	}

	sqsBk, ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			queues := sqsBk.TaggedQueues()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(queues))
			for _, q := range queues {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  q.ARN,
					ResourceType: "sqs:queue",
					Tags:         q.Tags,
				})
			}

			return out
		},
		"sqs",
		sqsBk.TagQueueByARN,
		sqsBk.UntagQueueByARN,
	)
}

func wireTaggingSNS(
	bk resourcegroupstaggingapibackend.StorageBackend,
	snsReg service.Registerable,
) {
	snsH, ok := snsReg.(*snsbackend.Handler)
	if !ok {
		return
	}

	snsBk, ok := snsH.Backend.(*snsbackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			topics := snsBk.TaggedTopics()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(topics))
			for _, t := range topics {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  t.ARN,
					ResourceType: "sns:topic",
					Tags:         t.Tags,
				})
			}

			return out
		},
		"sns",
		snsBk.TagTopicByARN,
		snsBk.UntagTopicByARN,
	)
}

func wireTaggingLambda(
	bk resourcegroupstaggingapibackend.StorageBackend,
	lambdaReg service.Registerable,
) {
	lambdaH, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			fns := lambdaH.TaggedFunctions()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(fns))
			for _, f := range fns {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  f.ARN,
					ResourceType: "lambda:function",
					Tags:         f.Tags,
				})
			}

			return out
		},
		"lambda",
		lambdaH.TagFunctionByARN,
		lambdaH.UntagFunctionByARN,
	)
}

func wireTaggingKMS(
	bk resourcegroupstaggingapibackend.StorageBackend,
	kmsReg service.Registerable,
) {
	kmsH, ok := kmsReg.(*kmsbackend.Handler)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			keys := kmsH.TaggedKeys()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(keys))
			for _, k := range keys {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  k.ARN,
					ResourceType: "kms:key",
					Tags:         k.Tags,
				})
			}

			return out
		},
		"kms",
		kmsH.TagKeyByARN,
		kmsH.UntagKeyByARN,
	)
}

func wireTaggingSM(bk resourcegroupstaggingapibackend.StorageBackend, smReg service.Registerable) {
	smH, ok := smReg.(*secretsmanagerbackend.Handler)
	if !ok {
		return
	}

	smBk, ok := smH.Backend.(*secretsmanagerbackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(_ context.Context) []resourcegroupstaggingapibackend.TaggedResource {
			secrets := smBk.TaggedSecrets()
			out := make([]resourcegroupstaggingapibackend.TaggedResource, 0, len(secrets))
			for _, s := range secrets {
				out = append(out, resourcegroupstaggingapibackend.TaggedResource{
					ResourceARN:  s.ARN,
					ResourceType: "secretsmanager:secret",
					Tags:         s.Tags,
				})
			}

			return out
		},
		"secretsmanager",
		smBk.TagSecretByARN,
		smBk.UntagSecretByARN,
	)
}

func startServer(ctx context.Context, port string, e *echo.Echo, tlsCfg tlsSettings) error {
	log := logger.Load(ctx)

	if port[0] != ':' {
		port = ":" + port
	}

	scheme := "http"
	if tlsCfg.enabled {
		scheme = "https"
	}

	log.InfoContext(ctx, "Starting Gopherstack (DynamoDB + S3)", "port", port, "scheme", scheme)
	log.InfoContext(ctx, "  DynamoDB endpoint", "url", scheme+"://localhost"+port)
	log.InfoContext(ctx, "  S3 endpoint      ", "url", scheme+"://localhost"+port+" (path-style)")
	log.InfoContext(ctx, "  Dashboard        ", "url", scheme+"://localhost"+port+"/dashboard")

	server := &http.Server{
		Addr:    port,
		Handler: e,
		// Protocols set below; under TLS we omit the unencrypted-h2 setting so
		// the standard h2 ALPN negotiation applies.
		ReadTimeout:       defaultTimeout,
		ReadHeaderTimeout: defaultReadHeaderTimeout, // Security best practice
		// WriteTimeout intentionally 0: long-lived ConnectRPC streams
		// (StreamConsole, StreamMetrics) must outlive the per-request budget.
		// The default 30s WriteTimeout cut the metrics stream mid-frame after
		// ~30s, surfacing as ERR_INCOMPLETE_CHUNK in the browser. Slow-write
		// attacks are mitigated by ReadHeaderTimeout above; for an in-memory
		// dev simulator this is an acceptable trade-off.
		WriteTimeout: 0,
		IdleTimeout:  defaultTimeout,
	}

	if !tlsCfg.enabled {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		server.Protocols = protocols
	}

	errChan := make(chan error, 1)
	go func() {
		if err := serveHTTP(server, tlsCfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.InfoContext(ctx, "Shutting down server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.ErrorContext(ctx, "Server shutdown failed", "error", err)

			return err
		}

		return nil
	case err := <-errChan:
		log.ErrorContext(ctx, "Failed to start server", "error", err)

		return err
	}
}

// serveHTTP starts the server, choosing HTTP, file-based TLS, or self-signed TLS
// based on tlsCfg. It blocks until the server stops.
func serveHTTP(server *http.Server, tlsCfg tlsSettings) error {
	if !tlsCfg.enabled {
		return server.ListenAndServe()
	}

	if tlsCfg.certFile != "" && tlsCfg.keyFile != "" {
		return server.ListenAndServeTLS(tlsCfg.certFile, tlsCfg.keyFile)
	}

	// No cert supplied: generate a self-signed certificate in memory.
	cert, err := generateSelfSignedCert()
	if err != nil {
		return fmt.Errorf("generate self-signed certificate: %w", err)
	}

	server.TLSConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Empty cert/key paths => server uses TLSConfig.Certificates.
	return server.ListenAndServeTLS("", "")
}

// generateSelfSignedCert creates an in-memory self-signed certificate valid for
// localhost / 127.0.0.1 / ::1, suitable for an opt-in dev HTTPS listener.
func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), selfSignedSerialBits)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}

	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "gopherstack", Organization: []string{"gopherstack"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(selfSignedValidity),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{localhostName},
		IPAddresses:           []net.IP{net.IPv4(loopbackIPv4Octet, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

// buildLogger converts the CLI log-level string to a [slog.Logger].
func buildLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch strings.ToLower(strings.TrimSpace(level)) {
	case logLevelDebug:
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	return logger.NewLogger(slogLevel)
}

// healthResponse is the JSON body returned by the health endpoint.
type healthResponse struct {
	// Status is always "ok" when the server is running.
	Status string `json:"status"`
	// Version is the build-time version of Gopherstack.
	Version string `json:"version"`
	// Services lists all registered mock AWS services.
	Services []string `json:"services"`
	// Goroutines is the current number of live goroutines (runtime.NumGoroutine).
	// A sustained upward trend indicates a goroutine leak.
	Goroutines int `json:"goroutines"`
	// HeapAllocB is the number of heap bytes currently allocated (runtime.MemStats.HeapAlloc).
	HeapAllocB uint64 `json:"heap_alloc_bytes"`
	// HeapInuseB is the number of heap bytes in use (runtime.MemStats.HeapInuse).
	HeapInuseB uint64 `json:"heap_inuse_bytes"`
	// NumGC is the total number of completed GC cycles (runtime.MemStats.NumGC).
	NumGC uint32 `json:"num_gc"`
}

func setupChaosAndRegistry(
	e *echo.Echo,
	log *slog.Logger,
	cli *CLI,
	services []service.Registerable,
) error {
	faultStore := cli.faultStore
	chaosGroup := e.Group("/_gopherstack/chaos")
	wireFISFaultStore(cli.fisHandler, faultStore)

	registry, err := setupRegistry(
		e,
		log,
		services,
		cli.LatencyMs,
		cli.EnforceIAM,
		cli.GetGlobalConfig(),
		faultStore,
	)
	if err != nil {
		return err
	}

	chaos.RegisterRoutes(chaosGroup, faultStore, registry)

	return nil
}

func setupRegistry(
	e *echo.Echo,
	log *slog.Logger,
	services []service.Registerable,
	latencyMs int,
	enforceIAM bool,
	globalCfg *config.GlobalConfig,
	faultStore *chaos.FaultStore,
) (*service.Registry, error) {
	registry := service.NewRegistry()

	if latencyMs > 0 {
		registry.SetLatencyMs(latencyMs)
	}

	// Wire the live CloudTrail backend (if registered) as the registry's global
	// management-event recorder. This makes every mutating call to every
	// registered service show up in CloudTrail LookupEvents, without any
	// per-service integration code.
	if ctRecorder := findCloudTrailRecorder(services); ctRecorder != nil {
		registry.SetCloudTrailRecorder(ctRecorder)
	}

	// Chaos middleware runs outside the telemetry wrapper (as a global middleware).
	// It extracts service/region/operation directly from the HTTP request headers so
	// it does not depend on context values that are only set by the telemetry wrapper.
	registry.Use(chaos.Middleware(faultStore))

	if enforceIAM {
		iamBackend := findIAMBackend(services)
		if iamBackend != nil {
			log.Info("IAM policy enforcement enabled")

			ecfg := iambackend.EnforcementConfig{
				Global:            globalCfg,
				ResourceProviders: buildResourcePolicyProviders(services),
				ActionExtractors:  buildActionExtractors(services),
			}

			registry.Use(service.Middleware(iambackend.EnforcementMiddleware(iamBackend, ecfg)))
		} else {
			log.Warn("IAM enforcement requested but IAM backend not found; enforcement disabled")
		}
	}

	for _, svc := range services {
		if err := registry.Register(svc); err != nil {
			log.Error("Failed to register service", "service", svc.Name(), "error", err)

			return nil, err
		}
	}

	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return registry, nil
}

// findCloudTrailRecorder locates the live CloudTrail backend from the service
// list, so the registry can wire it as its global management-event recorder
// instead of constructing a second, disconnected CloudTrail backend.
func findCloudTrailRecorder(services []service.Registerable) service.CloudTrailRecorder {
	for _, svc := range services {
		if rec, ok := svc.(service.CloudTrailRecorder); ok {
			return rec
		}
	}

	return nil
}

// findIAMBackend locates the IAM EnforcementBackend from the service list.
func findIAMBackend(services []service.Registerable) iambackend.EnforcementBackend {
	for _, svc := range services {
		if h, ok := svc.(*iambackend.Handler); ok {
			if b, ok2 := h.Backend.(iambackend.EnforcementBackend); ok2 {
				return b
			}
		}
	}

	return nil
}

// buildActionExtractors collects ActionExtractor implementations from all registered
// services. Services that implement the iam.ActionExtractor interface are automatically
// included so their REST-API action mappings are used by the enforcement middleware.
func buildActionExtractors(services []service.Registerable) []iambackend.ActionExtractor {
	var extractors []iambackend.ActionExtractor

	for _, svc := range services {
		if ae, ok := svc.(iambackend.ActionExtractor); ok {
			extractors = append(extractors, ae)
		}
	}

	return extractors
}

// buildResourcePolicyProviders builds a list of ResourcePolicyProvider adapters
// from the registered service backends that support resource-based policies.
func buildResourcePolicyProviders(
	services []service.Registerable,
) []iambackend.ResourcePolicyProvider {
	var providers []iambackend.ResourcePolicyProvider

	for _, svc := range services {
		switch h := svc.(type) {
		case *s3backend.S3Handler:
			if b, ok := h.Backend.(s3PolicyBackend); ok {
				providers = append(providers, &s3PolicyAdapter{backend: b})
			}
		case *sqsbackend.Handler:
			if b, ok := h.Backend.(sqsPolicyBackend); ok {
				providers = append(providers, &sqsPolicyAdapter{backend: b})
			}
		}
	}

	return providers
}

// s3PolicyBackend is the minimal S3 backend interface needed for bucket policies.
type s3PolicyBackend interface {
	GetBucketPolicy(ctx context.Context, bucketName string) (string, error)
}

// sqsPolicyBackend is the minimal SQS backend interface needed for queue policies.
type sqsPolicyBackend interface {
	GetQueueAttributes(
		input *sqsbackend.GetQueueAttributesInput,
	) (*sqsbackend.GetQueueAttributesOutput, error)
}

// s3PolicyAdapter wraps an S3 backend to implement ResourcePolicyProvider.
// It handles ARNs of the form arn:aws:s3:::bucket or arn:aws:s3:::bucket/key.
type s3PolicyAdapter struct {
	backend s3PolicyBackend
}

func (a *s3PolicyAdapter) GetResourcePolicy(
	ctx context.Context,
	resourceARN string,
) (string, error) {
	const prefix = "arn:aws:s3:::"
	if !strings.HasPrefix(resourceARN, prefix) {
		return "", nil
	}

	path := strings.TrimPrefix(resourceARN, prefix)
	bucketName, _, _ := strings.Cut(path, "/")

	if bucketName == "" {
		return "", nil
	}

	return a.backend.GetBucketPolicy(ctx, bucketName)
}

// sqsPolicyAdapter wraps a SQS backend to implement ResourcePolicyProvider.
// It handles ARNs of the form arn:aws:sqs:region:account:queue-name.
type sqsPolicyAdapter struct {
	backend sqsPolicyBackend
}

func (a *sqsPolicyAdapter) GetResourcePolicy(
	_ context.Context,
	resourceARN string,
) (string, error) {
	const prefix = "arn:aws:sqs:"
	if !strings.HasPrefix(resourceARN, prefix) {
		return "", nil
	}

	// arn:aws:sqs:region:account:queue-name → extract queue name (last segment)
	parts := strings.Split(resourceARN, ":")
	const arnParts = 6
	if len(parts) < arnParts {
		return "", nil
	}

	queueName := parts[len(parts)-1]
	if queueName == "" {
		return "", nil
	}

	accountID := parts[4]
	queueURL := "http://localhost/" + accountID + "/" + queueName

	out, err := a.backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
		QueueURL:       queueURL,
		AttributeNames: []string{"Policy"},
	})
	if err != nil {
		return "", err
	}

	return out.Attributes["Policy"], nil
}

// startEmbeddedDNS creates and starts the embedded DNS server.
// Configuration errors and startup failures are logged as warnings; the server
// continues to run without DNS in those cases.
func startEmbeddedDNS(ctx context.Context, addr, resolveIP string) *gopherDNS.Server {
	log := logger.Load(ctx)

	dnsSrv, err := gopherDNS.New(gopherDNS.Config{
		ListenAddr: addr,
		ResolveIP:  resolveIP,
		Logger:     log,
	})
	if err != nil {
		log.WarnContext(ctx, "DNS server disabled (config error)", "error", err)

		return nil
	}

	if startErr := dnsSrv.Start(ctx); startErr != nil {
		log.WarnContext(ctx, "DNS server failed to start", "error", startErr)

		return nil
	}

	log.InfoContext(ctx, "DNS server started", "addr", addr)

	return dnsSrv
}

// wireLambdaDNS sets the DNS registrar on the Lambda backend so function URL
// hostnames are automatically registered when CreateFunctionUrlConfig is called.
func wireLambdaDNS(lambdaReg service.Registerable, dns lambdabackend.DNSRegistrar) {
	if lambdaReg == nil || dns == nil {
		return
	}

	lambdaH, ok := lambdaReg.(*lambdabackend.Handler)
	if !ok {
		return
	}

	if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
		lambdaBk.SetDNSRegistrar(dns)
	}
}

// wireRoute53DNS sets the DNS registrar on the Route 53 backend so that
// A and CNAME record sets are automatically registered in the embedded DNS server.
func wireRoute53DNS(r53Reg service.Registerable, dns route53backend.DNSRegistrar) {
	if r53Reg == nil || dns == nil {
		return
	}

	r53H, ok := r53Reg.(*route53backend.Handler)
	if !ok {
		return
	}

	bk, isMem := r53H.Backend.(*route53backend.InMemoryBackend)
	if isMem {
		bk.SetDNSRegistrar(dns)
	}
}

// wireRDSDNS sets the DNS registrar on the RDS backend so that instance hostnames
// are automatically registered with the embedded DNS server.
func wireRDSDNS(rdsReg service.Registerable, dns rdsbackend.DNSRegistrar) {
	if rdsReg == nil || dns == nil {
		return
	}

	rdsH, ok := rdsReg.(*rdsbackend.Handler)
	if !ok {
		return
	}

	rdsH.Backend.SetDNSRegistrar(dns)
}

// wireRedshiftDNS sets the DNS registrar on the Redshift backend so that cluster
// hostnames are automatically registered with the embedded DNS server.
func wireRedshiftDNS(redshiftReg service.Registerable, dns redshiftbackend.DNSRegistrar) {
	if redshiftReg == nil || dns == nil {
		return
	}

	redshiftH, ok := redshiftReg.(*redshiftbackend.Handler)
	if !ok {
		return
	}

	redshiftH.Backend.SetDNSRegistrar(dns)
}

// wireOpenSearchDNS sets the DNS registrar on the OpenSearch backend so that domain
// hostnames are automatically registered with the embedded DNS server.
func wireOpenSearchDNS(osReg service.Registerable, dns opensearchbackend.DNSRegistrar) {
	if osReg == nil || dns == nil {
		return
	}

	osH, ok := osReg.(*opensearchbackend.Handler)
	if !ok {
		return
	}

	bk, ok := osH.Backend.(*opensearchbackend.InMemoryBackend)
	if !ok {
		return
	}

	bk.SetDNSRegistrar(dns)
}

// wireElasticsearchDNS sets the DNS registrar on the Elasticsearch backend so that domain
// hostnames are automatically registered with the embedded DNS server.
func wireElasticsearchDNS(esReg service.Registerable, dns elasticsearchbackend.DNSRegistrar) {
	if esReg == nil || dns == nil {
		return
	}

	esH, ok := esReg.(*elasticsearchbackend.Handler)
	if !ok {
		return
	}

	esH.Backend.SetDNSRegistrar(dns)
}

// wireElastiCacheDNS sets the DNS registrar on the ElastiCache backend so
// cache cluster endpoints use AWS-style hostnames registered in the embedded DNS.
func wireElastiCacheDNS(ecReg service.Registerable, dns elasticachebackend.DNSRegistrar) {
	if ecReg == nil || dns == nil {
		return
	}

	ecH, ok := ecReg.(*elasticachebackend.Handler)
	if !ok {
		return
	}

	if ecBk, bkOk := ecH.Backend.(*elasticachebackend.InMemoryBackend); bkOk {
		ecBk.SetDNSRegistrar(dns)
	}
}

// wireEC2DNS sets the DNS registrar on the EC2 backend so that the synthetic
// public DNS hostnames generated by the docker compute provider can be
// resolved from outside the gopherstack process.
func wireEC2DNS(ec2Reg service.Registerable, dns ec2backend.DNSRegistrar) {
	if ec2Reg == nil || dns == nil {
		return
	}

	ec2H, ok := ec2Reg.(*ec2backend.Handler)
	if !ok {
		return
	}

	if ec2Bk, bkOk := ec2H.Backend.(*ec2backend.InMemoryBackend); bkOk {
		ec2Bk.SetDNSRegistrar(dns)
	}
}

// wireFirehoseDelivery connects the Firehose backend to S3 and Lambda so that
// buffered records are delivered to the configured S3 bucket, and optionally
// transformed by a Lambda function before delivery.
func wireFirehoseDelivery(firehoseReg, s3Reg, lambdaReg service.Registerable) {
	firehoseH, ok := firehoseReg.(*firehosebackend.Handler)
	if !ok {
		return
	}

	if s3H, s3Ok := s3Reg.(*s3backend.S3Handler); s3Ok {
		if s3Bk, bkOk := s3H.Backend.(*s3backend.InMemoryBackend); bkOk {
			if fhBk, fhOk := firehoseH.Backend.(*firehosebackend.InMemoryBackend); fhOk {
				fhBk.SetS3Backend(s3Bk)
			}
		}
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bkOk := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			if fhBk, fhOk := firehoseH.Backend.(*firehosebackend.InMemoryBackend); fhOk {
				fhBk.SetLambdaBackend(lambdaBk)
			}
		}
	}
}

// extractServiceName finds the service name for a given Echo context by checking
// which service's route matcher matches the request.
func extractServiceName(c *echo.Context, services []service.Registerable) string {
	for _, svc := range services {
		if svc.RouteMatcher()(c) {
			return svc.Name()
		}
	}

	return ""
}

// setupPersistence registers all persistable services with the manager and optionally restores state.
func setupPersistence(
	ctx context.Context,
	m *persistence.Manager,
	services []service.Registerable,
	restore bool,
) {
	type persistable interface {
		Snapshot() []byte
		Restore([]byte) error
	}

	for _, svc := range services {
		if p, ok := svc.(persistable); ok {
			m.Register(svc.Name(), p)
		}
	}

	if restore {
		m.RestoreAll(ctx)
	}
}

// initPersistenceManager creates and configures a persistence.Manager from the CLI config.
// If persistence is disabled it returns a manager backed by a NullStore.
func initPersistenceManager(ctx context.Context, cli *CLI) (*persistence.Manager, error) {
	log := logger.Load(ctx)
	var store persistence.Store = persistence.NullStore{}

	if cli.Persist && !cli.Demo {
		fs, err := cli.createPersistenceStore()
		if err != nil {
			return nil, fmt.Errorf("persistence: create file store: %w", err)
		}

		store = fs
		log.InfoContext(ctx, "Persistence enabled", "data_dir", cli.resolvedDataDir())
	}

	return persistence.NewManager(store), nil
}

// loadDemoData loads demo data into the services.
func loadDemoData(ctx context.Context, cli *CLI) {
	log := logger.Load(ctx)
	log.InfoContext(ctx, "Loading demo data...")

	err := demo.LoadData(ctx, &demo.Clients{
		DynamoDB:       cli.ddbClient,
		S3:             cli.s3Client,
		SQS:            cli.sqsClient,
		SNS:            cli.snsClient,
		IAM:            cli.iamClient,
		STS:            cli.stsClient,
		SSM:            cli.ssmClient,
		KMS:            cli.kmsClient,
		SecretsManager: cli.secretsManagerClient,
		ECR:            cli.ecrClient,
		AppSync:        cli.appSyncSdkClient,
		Amplify:        cli.amplifyClient,
		ECS:            cli.ecsClient,
		EKS:            cli.eksClient,
		IoT:            cli.iotClient,
		CodeDeploy:     cli.codeDeployClient,
		CodePipeline:   cli.codePipelineSDKClient,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to load demo data", "error", err)
	}

	seedAppConfigDataDemoProfiles(ctx, cli.appConfigDataHandler, log)
	seedBedrockRuntimeDemoInvocations(ctx, cli.bedrockruntimeHandler, log)
	seedRoute53ResolverDemoData(ctx, cli.route53resolverHandler, log)
	seedEMRServerlessDemoData(ctx, cli.emrserverlessHandler, log)
}

// seedAppConfigDataDemoProfiles seeds demo configuration profiles for visual dashboard inspection.
// AppConfigData has no AWS SDK write API, so profiles are seeded directly via the backend.
func seedAppConfigDataDemoProfiles(ctx context.Context, h service.Registerable, log *slog.Logger) {
	acdHandler, ok := h.(*appconfigdatabackend.Handler)
	if !ok || acdHandler == nil {
		log.DebugContext(ctx, "AppConfigData handler not available; skipping demo profile seeding")

		return
	}

	profiles := []struct {
		app, env, profile, content, contentType string
	}{
		{
			app: demoAppName, env: envProduction, profile: "feature-flags",
			content:     `{"featureFlagX":true,"enableNewUI":false,"maxRetries":3}`,
			contentType: contentTypeJSON,
		},
		{
			app: demoAppName, env: envProduction, profile: "rate-limits",
			content:     `{"requestsPerMinute":100,"burstLimit":200}`,
			contentType: contentTypeJSON,
		},
		{
			app: demoAppName, env: "staging", profile: "feature-flags",
			content:     `{"featureFlagX":true,"enableNewUI":true,"maxRetries":5}`,
			contentType: contentTypeJSON,
		},
	}

	for _, p := range profiles {
		if err := acdHandler.Backend.SetConfiguration(p.app, p.env, p.profile, p.content, p.contentType); err != nil {
			log.WarnContext(ctx, "Failed to seed AppConfigData profile", "error", err)
		}
	}

	log.InfoContext(ctx, "Seeded AppConfigData demo profiles", "count", len(profiles))
}

// seedBedrockRuntimeDemoInvocations seeds demo invocations for visual dashboard inspection.
// BedrockRuntime has no AWS SDK write API, so invocations are seeded directly via the backend.
func seedBedrockRuntimeDemoInvocations(
	ctx context.Context,
	h service.Registerable,
	log *slog.Logger,
) {
	brtHandler, ok := h.(*bedrockruntimebackend.Handler)
	if !ok || brtHandler == nil {
		log.DebugContext(
			ctx,
			"BedrockRuntime handler not available; skipping demo invocation seeding",
		)

		return
	}

	brtHandler.Backend.RecordInvocation(
		"InvokeModel",
		"anthropic.claude-v2",
		`{"prompt": "Human: What is the capital of France?\n\nAssistant:"}`,
		`{"completion": " Paris is the capital of France.", "stop_reason": "end_turn"}`,
	)
	converseOutput := `{"output": {"message": {"role": "assistant", ` +
		`"content": [{"text": "Hello! How can I help you today?"}]}}, "stopReason": "end_turn"}`
	brtHandler.Backend.RecordInvocation(
		"Converse",
		"anthropic.claude-3-sonnet-20240229-v1:0",
		`{"messages": [{"role": "user", "content": [{"text": "Hello!"}]}]}`,
		converseOutput,
	)

	log.InfoContext(ctx, "Seeded BedrockRuntime demo invocations")
}

// seedRoute53ResolverDemoData seeds demo resources for visual dashboard inspection.
func seedRoute53ResolverDemoData(ctx context.Context, h service.Registerable, log *slog.Logger) {
	r53rHandler, ok := h.(*route53resolverbackend.Handler)
	if !ok || r53rHandler == nil {
		log.DebugContext(ctx, "Route53Resolver handler not available; skipping demo seeding")

		return
	}

	b, ok2 := r53rHandler.Backend.(*route53resolverbackend.InMemoryBackend)
	if !ok2 || b == nil {
		log.DebugContext(
			ctx,
			"Route53Resolver backend type assertion failed; skipping demo seeding",
		)

		return
	}

	ep := b.AddEndpointInternal("corp-inbound", route53resolverbackend.DirectionInbound)
	b.AddEndpointInternal("corp-outbound", route53resolverbackend.DirectionOutbound)
	r := b.AddRuleInternal("corp-forward", "corp.internal.", route53resolverbackend.RuleTypeForward)
	_ = r
	if ep != nil {
		b.AddRuleInternalWithEndpoint(
			"vpc-lookup",
			"internal.example.com.",
			route53resolverbackend.RuleTypeForward,
			ep.ID,
		)
	}
	grp := b.AddFirewallRuleGroupInternal("BlockMalware-Group")
	b.AddFirewallRuleGroupInternal("AlertSuspicious-Group")
	dl := b.AddFirewallDomainListInternal("malware-domains")
	b.AddFirewallDomainListInternal("phishing-domains")
	if grp != nil && dl != nil {
		b.AddFirewallRuleInternal(
			grp.ID,
			"block-malware",
			"BLOCK",
			dl.ID,
			route53resolverbackend.FirewallPriorityDefault,
		)
	}
	b.AddQueryLogConfigInternal(
		"vpc-query-logs",
		"arn:aws:logs:us-east-1:000000000000:log-group:/resolver/queries",
	)

	log.InfoContext(ctx, "Seeded Route53Resolver demo data")
}

// seedEMRServerlessDemoData seeds demo applications and job runs for visual dashboard inspection.
func seedEMRServerlessDemoData(ctx context.Context, h service.Registerable, log *slog.Logger) {
	emrHandler, ok := h.(*emrserverlessbackend.Handler)
	if !ok || emrHandler == nil {
		log.DebugContext(ctx, "EMR Serverless handler not available; skipping demo seeding")

		return
	}

	now := time.Now().UTC()

	// Create demo applications.
	spark := &emrserverlessbackend.Application{
		ApplicationID: "demo0spark1",
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0spark1",
		Name:          "spark-etl-app",
		Type:          "SPARK",
		ReleaseLabel:  "emr-6.10.0",
		State:         emrserverlessbackend.ApplicationStateStarted,
		Tags:          map[string]string{"team": "data-engineering", "env": envProduction},
		CreatedAt:     now.Add(-72 * time.Hour),
		UpdatedAt:     now.Add(-1 * time.Hour),
	}
	hive := &emrserverlessbackend.Application{
		ApplicationID: "demo0hive01",
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0hive01",
		Name:          "hive-analytics-app",
		Type:          "HIVE",
		ReleaseLabel:  "emr-6.8.0",
		State:         emrserverlessbackend.ApplicationStateStopped,
		Tags:          map[string]string{"team": "analytics"},
		CreatedAt:     now.Add(-168 * time.Hour),
		UpdatedAt:     now.Add(-48 * time.Hour),
	}
	emrHandler.Backend.AddApplicationInternal(spark)
	emrHandler.Backend.AddApplicationInternal(hive)

	// Create demo job runs for the Spark application.
	emrHandler.Backend.AddJobRunInternal(&emrserverlessbackend.JobRun{
		ApplicationID:    spark.ApplicationID,
		JobRunID:         "demo0jr001",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0spark1/jobruns/demo0jr001",
		Name:             "daily-etl-pipeline",
		State:            emrserverlessbackend.JobRunStateSuccess,
		ExecutionRoleArn: emrServerlessRoleARN,
		CreatedAt:        now.Add(-24 * time.Hour),
		UpdatedAt:        now.Add(-23 * time.Hour),
		Tags:             map[string]string{"run": "daily"},
	})
	emrHandler.Backend.AddJobRunInternal(&emrserverlessbackend.JobRun{
		ApplicationID:    spark.ApplicationID,
		JobRunID:         "demo0jr002",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0spark1/jobruns/demo0jr002",
		Name:             "hourly-aggregation",
		State:            emrserverlessbackend.JobRunStateSubmitted,
		ExecutionRoleArn: emrServerlessRoleARN,
		CreatedAt:        now.Add(-15 * time.Minute),
		UpdatedAt:        now.Add(-15 * time.Minute),
		Tags:             map[string]string{"run": "hourly"},
	})
	emrHandler.Backend.AddJobRunInternal(&emrserverlessbackend.JobRun{
		ApplicationID:    spark.ApplicationID,
		JobRunID:         "demo0jr003",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0spark1/jobruns/demo0jr003",
		Name:             "backfill-2024-q1",
		State:            emrserverlessbackend.JobRunStateFailed,
		StateDetails:     "SparkContext stopped due to OOM in executor",
		ExecutionRoleArn: emrServerlessRoleARN,
		CreatedAt:        now.Add(-48 * time.Hour),
		UpdatedAt:        now.Add(-47 * time.Hour),
		Tags:             map[string]string{},
	})
	emrHandler.Backend.AddJobRunInternal(&emrserverlessbackend.JobRun{
		ApplicationID:    spark.ApplicationID,
		JobRunID:         "demo0jr004",
		Arn:              "arn:aws:emr-serverless:us-east-1:000000000000:/applications/demo0spark1/jobruns/demo0jr004",
		Name:             "schema-migration-v2",
		State:            emrserverlessbackend.JobRunStateCancelled,
		StateDetails:     "Job run cancelled by user request",
		ExecutionRoleArn: emrServerlessRoleARN,
		CreatedAt:        now.Add(-12 * time.Hour),
		UpdatedAt:        now.Add(-12 * time.Hour),
		Tags:             map[string]string{},
	})

	const seededApps, seededJobRuns = 2, 4

	log.InfoContext(
		ctx,
		"Seeded EMR Serverless demo data",
		"applications",
		seededApps,
		"jobRuns",
		seededJobRuns,
	)
}

// in HTTP handlers, logs the panic and stack trace via slog, and returns an
// HTTP 500 response. Without this middleware, a single unhandled panic in any
// handler goroutine kills the entire server process immediately.
func panicRecoveryMiddleware() echo.MiddlewareFunc {
	const stackSize = 4 << 10 // 4 KB

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) (err error) {
			defer func() {
				if r := recover(); r != nil {
					// Re-panic for http.ErrAbortHandler — it is not a real error
					// and must propagate to signal the transport layer.
					if rErr, ok := r.(error); ok && errors.Is(rErr, http.ErrAbortHandler) {
						panic(r)
					}

					panicErr, ok := r.(error)
					if !ok {
						panicErr = recoveredPanicError{val: r}
					}

					stack := make([]byte, stackSize)
					n := runtime.Stack(stack, false)
					logger.Load(c.Request().Context()).Error("panic recovered in HTTP handler",
						"error", panicErr, "stack", string(stack[:n]))

					// Record a metric so dashboards can alert on panic spikes.
					telemetry.RecordWorkerTask("http", "PanicRecovery", "error")

					err = c.JSON(http.StatusInternalServerError, map[string]string{
						"message": "internal server error",
					})
				}
			}()

			return next(c)
		}
	}
}

// recoveredPanicError wraps a non-error panic value recovered by panicRecoveryMiddleware.
type recoveredPanicError struct {
	val any
}

func (e recoveredPanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.val)
}

// persistenceMiddleware returns an Echo middleware that schedules a debounced snapshot
// after each mutating request.
func persistenceMiddleware(
	m *persistence.Manager,
	services []service.Registerable,
) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			handlerErr := next(c)

			if isMutatingMethod(c.Request().Method) {
				if svcName := extractServiceName(c, services); svcName != "" {
					m.Notify(svcName)
				}
			}

			return handlerErr
		}
	}
}

// isMutatingMethod reports whether the HTTP method is a state-mutating method.
func isMutatingMethod(method string) bool {
	return method == http.MethodPost || method == http.MethodPut ||
		method == http.MethodPatch || method == http.MethodDelete
}

// wireFISFaultStore injects the chaos FaultStore into the FIS backend so that
// aws:fis:inject-api-* actions can create and remove fault rules during experiments.
func wireFISFaultStore(fisReg service.Registerable, store *chaos.FaultStore) {
	if fisReg == nil || store == nil {
		return
	}

	// Use type assertion to reach the FIS handler's SetFaultStore method.
	if h, ok := fisReg.(interface {
		SetFaultStore(*chaos.FaultStore)
	}); ok {
		h.SetFaultStore(store)
	}
}

// wireFISActionProviders collects all services implementing service.FISActionProvider
// and registers them with the FIS backend for auto-discovered action execution.
func wireFISActionProviders(fisReg service.Registerable, services []service.Registerable) {
	if fisReg == nil {
		return
	}

	type actionProviderSetter interface {
		SetActionProviders([]service.FISActionProvider)
	}

	setter, ok := fisReg.(actionProviderSetter)
	if !ok {
		return
	}

	var providers []service.FISActionProvider

	for _, svc := range services {
		if p, pOK := svc.(service.FISActionProvider); pOK {
			providers = append(providers, p)
		}
	}

	setter.SetActionProviders(providers)
}

// wireDynamoDBStreams connects the DynamoDB Streams handler to the DynamoDB in-memory backend
// so that both services share the same underlying stream state.
func wireDynamoDBStreams(ddbReg, streamsReg service.Registerable) {
	streamsH, ok := streamsReg.(*dynamodbstreamsbackend.Handler)
	if !ok {
		return
	}

	ddbH, ddbOk := ddbReg.(*ddbbackend.DynamoDBHandler)
	if !ddbOk {
		return
	}

	if ddbBk, bkOk := ddbH.Backend.(ddbbackend.StreamsBackend); bkOk {
		streamsH.Streams = ddbBk
	}

	streamsH.DefaultRegion = ddbH.DefaultRegion
}

// wireSchedulerRunner configures the Scheduler runner with Lambda, SQS, SNS, and StepFunctions
// target invokers so that schedule expressions actually fire their targets.
func wireSchedulerRunner(schedReg, lambdaReg, sqsReg, snsReg, sfnReg service.Registerable) {
	schedH, ok := schedReg.(*schedulerbackend.Handler)
	if !ok {
		return
	}

	runner := schedH.GetRunner()

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			runner.SetLambdaInvoker(&schedulerLambdaAdapter{backend: lambdaBk})
		}
	}

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bkOk := sqsH.Backend.(*sqsbackend.InMemoryBackend); bkOk {
			runner.SetSQSSender(&sqsSenderAdapter{backend: sqsBk})
		}
	}

	if snsH, snsOk := snsReg.(*snsbackend.Handler); snsOk {
		if snsBk, bkOk := snsH.Backend.(*snsbackend.InMemoryBackend); bkOk {
			runner.SetSNSPublisher(&snsPublisherAdapter{backend: snsBk})
		}
	}

	if sfnH, sfnOk := sfnReg.(*sfnbackend.Handler); sfnOk {
		if sfnBk, bkOk := sfnH.Backend.(*sfnbackend.InMemoryBackend); bkOk {
			runner.SetStepFunctionsStarter(&sfnStarterAdapter{backend: sfnBk})
		}
	}
}

// schedulerLambdaAdapter adapts the Lambda backend to the scheduler.LambdaInvoker interface.
type schedulerLambdaAdapter struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *schedulerLambdaAdapter) InvokeFunction(
	ctx context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	return a.backend.InvokeFunction(ctx, name, invocationType, payload)
}

// sfnStarterAdapter adapts the StepFunctions backend to the scheduler.StepFunctionsStarter interface.
type sfnStarterAdapter struct {
	backend *sfnbackend.InMemoryBackend
}

func (a *sfnStarterAdapter) StartExecution(stateMachineARN, name, input string) error {
	_, err := a.backend.StartExecution(stateMachineARN, name, input)

	return err
}

// wirePipesRunner configures the Pipes runner with SQS source reader and Lambda/StepFunctions
// target invokers so that running pipes actually forward records to their targets.
func wirePipesRunner(pipesReg, sqsReg, lambdaReg, sfnReg service.Registerable) {
	pipesH, ok := pipesReg.(*pipesbackend.Handler)
	if !ok {
		return
	}

	runner := pipesH.GetRunner()

	if sqsH, sqsOk := sqsReg.(*sqsbackend.Handler); sqsOk {
		if sqsBk, bkOk := sqsH.Backend.(*sqsbackend.InMemoryBackend); bkOk {
			runner.SetSQSReader(&pipesSQSReaderAdapter{backend: sqsBk})
		}
	}

	if lambdaH, lambdaOk := lambdaReg.(*lambdabackend.Handler); lambdaOk {
		if lambdaBk, bk2Ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bk2Ok {
			runner.SetLambdaInvoker(&schedulerLambdaAdapter{backend: lambdaBk})
		}
	}

	if sfnH, sfnOk := sfnReg.(*sfnbackend.Handler); sfnOk {
		if sfnBk, bkOk := sfnH.Backend.(*sfnbackend.InMemoryBackend); bkOk {
			runner.SetStepFunctionsStarter(&pipesSFNStarterAdapter{backend: sfnBk})
		}
	}
}

// pipesSQSReaderAdapter adapts the SQS InMemoryBackend to the pipes.PipeSQSReader interface.
type pipesSQSReaderAdapter struct {
	backend *sqsbackend.InMemoryBackend
}

func (a *pipesSQSReaderAdapter) ReceivePipeMessages(
	queueARN string,
	maxMessages int,
) ([]*pipesbackend.SQSMessage, error) {
	url := arnToSQSQueueURL(queueARN)

	msgs, err := a.backend.ReceiveMessagesLocal(url, maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*pipesbackend.SQSMessage, len(msgs))
	for i, m := range msgs {
		result[i] = &pipesbackend.SQSMessage{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			Attributes:    m.Attributes,
			MD5OfBody:     m.MD5OfBody,
		}
	}

	return result, nil
}

func (a *pipesSQSReaderAdapter) DeletePipeMessages(queueARN string, receiptHandles []string) error {
	url := arnToSQSQueueURL(queueARN)

	return a.backend.DeleteMessagesLocal(url, receiptHandles)
}

// pipesSFNStarterAdapter adapts the StepFunctions InMemoryBackend to the pipes.PipeStepFunctionsStarter interface.
type pipesSFNStarterAdapter struct {
	backend *sfnbackend.InMemoryBackend
}

func (a *pipesSFNStarterAdapter) StartExecution(stateMachineARN, name, input string) error {
	_, err := a.backend.StartExecution(stateMachineARN, name, input)

	return err
}
