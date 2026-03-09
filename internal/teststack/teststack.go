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
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
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
	rgtabackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
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
	Echo                         *echo.Echo
	S3Backend                    *s3backend.InMemoryBackend
	S3Handler                    *s3backend.S3Handler
	DDBHandler                   *ddbbackend.DynamoDBHandler
	IAMBackend                   *iambackend.InMemoryBackend
	IAMHandler                   *iambackend.Handler
	STSHandler                   *stsbackend.Handler
	SNSHandler                   *snsbackend.Handler
	SQSHandler                   *sqsbackend.Handler
	KMSHandler                   *kmsbackend.Handler
	SecretsManagerHandler        *smbackend.Handler
	LambdaHandler                *lambdabackend.Handler
	EventBridgeHandler           *ebbackend.Handler
	APIGatewayHandler            *apigwbackend.Handler
	CloudWatchLogsHandler        *cwlogsbackend.Handler
	StepFunctionsHandler         *sfnbackend.Handler
	CloudWatchHandler            *cwbackend.Handler
	CloudFormationHandler        *cfnbackend.Handler
	KinesisHandler               *kinesisbackend.Handler
	ElastiCacheHandler           *elasticachebackend.Handler
	Route53Handler               *route53backend.Handler
	SESHandler                   *sesbackend.Handler
	EC2Handler                   *ec2backend.Handler
	OpenSearchHandler            *opensearchbackend.Handler
	ACMHandler                   *acmbackend.Handler
	RedshiftHandler              *redshiftbackend.Handler
	RDSHandler                   *rdsbackend.Handler
	AWSConfigHandler             *awsconfigbackend.Handler
	S3ControlHandler             *s3controlbackend.Handler
	ResourceGroupsHandler        *resourcegroupsbackend.Handler
	ResourceGroupsTaggingHandler *rgtabackend.Handler
	SWFHandler                   *swfbackend.Handler
	FirehoseHandler              *firehosebackend.Handler
	SchedulerHandler             *schedulerbackend.Handler
	Route53ResolverHandler       *route53resolverbackend.Handler
	TranscribeHandler            *transcribebackend.Handler
	SupportHandler               *supportbackend.Handler
	CognitoIdentityHandler       *cognitoidentitybackend.Handler
	S3Client                     *s3.Client
	DDBClient                    *dynamodb.Client
	FaultStore                   *chaos.FaultStore
	Dashboard                    *dashboard.DashboardHandler
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
	ec2Hndlr *ec2backend.Handler,
	openSearchHndlr *opensearchbackend.Handler,
	acmHndlr *acmbackend.Handler,
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
	_ = registry.Register(ec2Hndlr)
	_ = registry.Register(openSearchHndlr)
	_ = registry.Register(acmHndlr)
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
}

// handlers bundles all service handlers created for a test stack.
type handlers struct {
	s3              *s3backend.S3Handler
	ddb             *ddbbackend.DynamoDBHandler
	ssm             *ssmbackend.Handler
	iam             *iambackend.Handler
	sts             *stsbackend.Handler
	sns             *snsbackend.Handler
	sqs             *sqsbackend.Handler
	kms             *kmsbackend.Handler
	sm              *smbackend.Handler
	lambda          *lambdabackend.Handler
	eb              *ebbackend.Handler
	apigw           *apigwbackend.Handler
	cwlogs          *cwlogsbackend.Handler
	sfn             *sfnbackend.Handler
	cw              *cwbackend.Handler
	cfn             *cfnbackend.Handler
	kinesis         *kinesisbackend.Handler
	elasticache     *elasticachebackend.Handler
	route53         *route53backend.Handler
	ses             *sesbackend.Handler
	ec2             *ec2backend.Handler
	opensearch      *opensearchbackend.Handler
	acm             *acmbackend.Handler
	redshift        *redshiftbackend.Handler
	rds             *rdsbackend.Handler
	awsconfig       *awsconfigbackend.Handler
	s3control       *s3controlbackend.Handler
	resourcegroups  *resourcegroupsbackend.Handler
	rgtagging       *rgtabackend.Handler
	swf             *swfbackend.Handler
	firehose        *firehosebackend.Handler
	scheduler       *schedulerbackend.Handler
	route53resolver *route53resolverbackend.Handler
	transcribe      *transcribebackend.Handler
	support         *supportbackend.Handler
	cognitoIdentity *cognitoidentitybackend.Handler
	iamBk           *iambackend.InMemoryBackend
	s3Bk            *s3backend.InMemoryBackend
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

	return handlers{
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
		ec2: ec2backend.NewHandler(
			ec2backend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		opensearch: opensearchbackend.NewHandler(
			opensearchbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		acm: acmbackend.NewHandler(
			acmbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		redshift: redshiftbackend.NewHandler(
			redshiftbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		rds: rdsbackend.NewHandler(
			rdsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		awsconfig: awsconfigbackend.NewHandler(awsconfigbackend.NewInMemoryBackend()),
		s3control: s3controlbackend.NewHandler(s3controlbackend.NewInMemoryBackend()),
		resourcegroups: resourcegroupsbackend.NewHandler(
			resourcegroupsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		rgtagging: rgtabackend.NewHandler(
			rgtabackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		swf: swfbackend.NewHandler(swfbackend.NewInMemoryBackend()),
		firehose: firehosebackend.NewHandler(
			firehosebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		scheduler: schedulerbackend.NewHandler(
			schedulerbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		route53resolver: route53resolverbackend.NewHandler(
			route53resolverbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
		),
		transcribe: transcribebackend.NewHandler(transcribebackend.NewInMemoryBackend()),
		support:    supportbackend.NewHandler(supportbackend.NewInMemoryBackend()),
		cognitoIdentity: cognitoidentitybackend.NewHandler(
			cognitoidentitybackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
			config.DefaultRegion,
		),
	}
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

	return dashboard.Config{
		DDBClient:          clients.DDB,
		S3Client:           clients.S3,
		SSMClient:          clients.SSM,
		DDBOps:             h.ddb,
		S3Ops:              h.s3,
		SSMOps:             h.ssm,
		IAMOps:             h.iam,
		STSOps:             h.sts,
		SNSOps:             h.sns,
		SQSOps:             h.sqs,
		KMSOps:             h.kms,
		SecretsManagerOps:  h.sm,
		LambdaOps:          h.lambda,
		EventBridgeOps:     h.eb,
		APIGatewayOps:      h.apigw,
		CloudWatchLogsOps:  h.cwlogs,
		StepFunctionsOps:   h.sfn,
		CloudWatchOps:      h.cw,
		CloudFormationOps:  h.cfn,
		KinesisOps:         h.kinesis,
		ElastiCacheOps:     h.elasticache,
		Route53Ops:         h.route53,
		SESOps:             h.ses,
		EC2Ops:             h.ec2,
		OpenSearchOps:      h.opensearch,
		ACMOps:             h.acm,
		RedshiftOps:        h.redshift,
		RDSOps:             h.rds,
		AWSConfigOps:       h.awsconfig,
		S3ControlOps:       h.s3control,
		ResourceGroupsOps:  h.resourcegroups,
		SWFOps:             h.swf,
		FirehoseOps:        h.firehose,
		SchedulerOps:       h.scheduler,
		Route53ResolverOps: h.route53resolver,
		TranscribeOps:      h.transcribe,
		SupportOps:         h.support,
		CognitoIdentityOps: h.cognitoIdentity,
		GlobalConfig:       config.GlobalConfig{AccountID: config.DefaultAccountID, Region: config.DefaultRegion},
		FaultStore:         fs,
		Logger:             slog.Default(),
	}, fs
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
		h.elasticache, h.route53, h.ses, h.ec2, h.opensearch,
		h.acm, h.redshift, h.rds, h.awsconfig, h.s3control, h.resourcegroups, h.rgtagging, h.swf, h.firehose,
		h.scheduler, h.route53resolver, h.transcribe, h.support, h.cognitoIdentity,
	)

	// Create AWS SDK clients routed through in-memory Echo, then wire dashboard.
	clients := newSDKClients(t, e)
	dashCfg, faultStore := newDashboardConfig(h, clients)
	dashHndlr := dashboard.NewHandler(dashCfg)
	_ = registry.Register(dashHndlr)

	// Mount the service router — this is the step that was previously easy to forget.
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return &Stack{
		Echo:                         e,
		S3Backend:                    h.s3Bk,
		S3Handler:                    h.s3,
		DDBHandler:                   h.ddb,
		IAMBackend:                   h.iamBk,
		IAMHandler:                   h.iam,
		STSHandler:                   h.sts,
		SNSHandler:                   h.sns,
		SQSHandler:                   h.sqs,
		KMSHandler:                   h.kms,
		SecretsManagerHandler:        h.sm,
		LambdaHandler:                h.lambda,
		EventBridgeHandler:           h.eb,
		APIGatewayHandler:            h.apigw,
		CloudWatchLogsHandler:        h.cwlogs,
		StepFunctionsHandler:         h.sfn,
		CloudWatchHandler:            h.cw,
		CloudFormationHandler:        h.cfn,
		KinesisHandler:               h.kinesis,
		ElastiCacheHandler:           h.elasticache,
		Route53Handler:               h.route53,
		SESHandler:                   h.ses,
		EC2Handler:                   h.ec2,
		OpenSearchHandler:            h.opensearch,
		ACMHandler:                   h.acm,
		RedshiftHandler:              h.redshift,
		RDSHandler:                   h.rds,
		AWSConfigHandler:             h.awsconfig,
		S3ControlHandler:             h.s3control,
		ResourceGroupsHandler:        h.resourcegroups,
		ResourceGroupsTaggingHandler: h.rgtagging,
		SWFHandler:                   h.swf,
		FirehoseHandler:              h.firehose,
		SchedulerHandler:             h.scheduler,
		Route53ResolverHandler:       h.route53resolver,
		TranscribeHandler:            h.transcribe,
		SupportHandler:               h.support,
		CognitoIdentityHandler:       h.cognitoIdentity,
		S3Client:                     clients.S3,
		DDBClient:                    clients.DDB,
		FaultStore:                   faultStore,
		Dashboard:                    dashHndlr,
	}
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
