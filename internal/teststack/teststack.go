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
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	smbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

const (
	// DynamoDB provisioned throughput for test tables.
	testReadCapacityUnits  = 5
	testWriteCapacityUnits = 5
)

// Stack holds a fully wired in-memory test stack with all services,
// the Echo router (correctly mounted), AWS SDK clients, and the dashboard handler.
type Stack struct {
	Echo                  *echo.Echo
	S3Backend             *s3backend.InMemoryBackend
	S3Handler             *s3backend.S3Handler
	DDBHandler            *ddbbackend.DynamoDBHandler
	IAMBackend            *iambackend.InMemoryBackend
	IAMHandler            *iambackend.Handler
	STSHandler            *stsbackend.Handler
	SNSHandler            *snsbackend.Handler
	SQSHandler            *sqsbackend.Handler
	KMSHandler            *kmsbackend.Handler
	SecretsManagerHandler *smbackend.Handler
	LambdaHandler         *lambdabackend.Handler
	S3Client              *s3.Client
	DDBClient             *dynamodb.Client
	Dashboard             *dashboard.DashboardHandler
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
		awscfg.WithRegion("us-east-1"),
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
		nil, nil, lambdabackend.DefaultSettings(), "000000000000", "us-east-1", slog.Default(),
	)
	h := lambdabackend.NewHandler(bk, slog.Default())
	h.AccountID = "000000000000"
	h.DefaultRegion = "us-east-1"

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
}

// New creates a fully wired integration stack for testing.
// It sets up all in-memory backends, handlers, the service registry with router,
// AWS SDK clients (routed back through Echo via InMemClient), and the dashboard.
func New(t *testing.T) *Stack {
	t.Helper()

	// Create in-memory backends and handlers for all services.
	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3Hndlr := s3backend.NewHandler(s3Bk, slog.Default())
	ddbBk := ddbbackend.NewInMemoryDB()
	ddbHndlr := ddbbackend.NewHandler(ddbBk, slog.Default())
	ssmBk := ssmbackend.NewInMemoryBackend()
	ssmHndlr := ssmbackend.NewHandler(ssmBk, slog.Default())
	iamBk := iambackend.NewInMemoryBackend()
	iamHndlr := iambackend.NewHandler(iamBk, slog.Default())
	stsBk := stsbackend.NewInMemoryBackend()
	stsHndlr := stsbackend.NewHandler(stsBk, slog.Default())
	snsBk := snsbackend.NewInMemoryBackend()
	snsHndlr := snsbackend.NewHandler(snsBk, slog.Default())
	sqsBk := sqsbackend.NewInMemoryBackend()
	sqsHndlr := sqsbackend.NewHandler(sqsBk, slog.Default())
	kmsBk := kmsbackend.NewInMemoryBackend()
	kmsHndlr := kmsbackend.NewHandler(kmsBk, slog.Default())
	smBk := smbackend.NewInMemoryBackend()
	smHndlr := smbackend.NewHandler(smBk, slog.Default())
	lambdaHndlr := newLambdaHandler()

	// Set up Echo with service registry and router.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))

	registry := service.NewRegistry(slog.Default())
	registerServices(
		registry,
		ddbHndlr,
		s3Hndlr,
		ssmHndlr,
		iamHndlr,
		stsHndlr,
		snsHndlr,
		sqsHndlr,
		kmsHndlr,
		smHndlr,
		lambdaHndlr,
	)

	// Create AWS SDK clients routed through in-memory Echo.
	clients := newSDKClients(t, e)
	ddbClient, s3Client, ssmClient := clients.DDB, clients.S3, clients.SSM

	// Create dashboard handler and register it.
	dashHndlr := dashboard.NewHandler(dashboard.Config{
		DDBClient:         ddbClient,
		S3Client:          s3Client,
		SSMClient:         ssmClient,
		DDBOps:            ddbHndlr,
		S3Ops:             s3Hndlr,
		SSMOps:            ssmHndlr,
		IAMOps:            iamHndlr,
		STSOps:            stsHndlr,
		SNSOps:            snsHndlr,
		SQSOps:            sqsHndlr,
		KMSOps:            kmsHndlr,
		SecretsManagerOps: smHndlr,
		LambdaOps:         lambdaHndlr,
		GlobalConfig: config.GlobalConfig{
			AccountID: "000000000000",
			Region:    "us-east-1",
		},
		Logger: slog.Default(),
	})
	_ = registry.Register(dashHndlr)

	// Mount the service router — this is the step that was previously easy to forget.
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return &Stack{
		Echo:                  e,
		S3Backend:             s3Bk,
		S3Handler:             s3Hndlr,
		DDBHandler:            ddbHndlr,
		IAMBackend:            iamBk,
		IAMHandler:            iamHndlr,
		STSHandler:            stsHndlr,
		SNSHandler:            snsHndlr,
		SQSHandler:            sqsHndlr,
		KMSHandler:            kmsHndlr,
		SecretsManagerHandler: smHndlr,
		LambdaHandler:         lambdaHndlr,
		S3Client:              s3Client,
		DDBClient:             ddbClient,
		Dashboard:             dashHndlr,
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
