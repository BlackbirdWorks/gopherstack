package teststack

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

// Stack holds a fully wired in-memory test stack with all services,
// the Echo router (correctly mounted), AWS SDK clients, and the dashboard handler.
type Stack struct {
	Echo       *echo.Echo
	S3Backend  *s3backend.InMemoryBackend
	S3Handler  *s3backend.S3Handler
	DDBHandler *ddbbackend.DynamoDBHandler
	IAMBackend *iambackend.InMemoryBackend
	IAMHandler *iambackend.Handler
	STSHandler *stsbackend.Handler
	SNSHandler *snsbackend.Handler
	SQSHandler *sqsbackend.Handler
	S3Client   *s3.Client
	DDBClient  *dynamodb.Client
	Dashboard  *dashboard.DashboardHandler
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

	// Set up Echo with service registry and router.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))

	registry := service.NewRegistry(slog.Default())
	_ = registry.Register(ddbHndlr)
	_ = registry.Register(s3Hndlr)
	_ = registry.Register(ssmHndlr)
	_ = registry.Register(iamHndlr)
	_ = registry.Register(stsHndlr)
	_ = registry.Register(snsHndlr)
	_ = registry.Register(sqsHndlr)

	// Create AWS SDK clients routed through in-memory Echo.
	inMemClient := &dashboard.InMemClient{Handler: e}

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
		awscfg.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})
	ssmClient := ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})

	// Create dashboard handler and register it.
	dashHndlr := dashboard.NewHandler(dashboard.Config{
		DDBClient: ddbClient,
		S3Client:  s3Client,
		SSMClient: ssmClient,
		DDBOps:    ddbHndlr,
		S3Ops:     s3Hndlr,
		SSMOps:    ssmHndlr,
		IAMOps:    iamHndlr,
		STSOps:    stsHndlr,
		SNSOps:    snsHndlr,
		SQSOps:    sqsHndlr,
		Logger:    slog.Default(),
	})
	_ = registry.Register(dashHndlr)

	// Mount the service router — this is the step that was previously easy to forget.
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return &Stack{
		Echo:       e,
		S3Backend:  s3Bk,
		S3Handler:  s3Hndlr,
		DDBHandler: ddbHndlr,
		IAMBackend: iamBk,
		IAMHandler: iamHndlr,
		STSHandler: stsHndlr,
		SNSHandler: snsHndlr,
		SQSHandler: sqsHndlr,
		S3Client:   s3Client,
		DDBClient:  ddbClient,
		Dashboard:  dashHndlr,
	}
}
