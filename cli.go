package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	snsevents "github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

const (
	defaultPort     = "8000"
	defaultRegion   = "us-east-1"
	defaultTimeout  = 30 * time.Second
	shutdownTimeout = 5 * time.Second
)

// CLI holds all command-line / environment-variable configuration for Gopherstack.
type CLI struct {
	SSM                   struct{}            `embed:"" prefix:"ssm-"`
	SecretsManager        struct{}            `embed:"" prefix:"secretsmanager-"`
	KMS                   struct{}            `embed:"" prefix:"kms-"`
	SQS                   sqsbackend.Settings `embed:"" prefix:"sqs-"`
	SNS                   struct{}            `embed:"" prefix:"sns-"`
	STS                   struct{}            `embed:"" prefix:"sts-"`
	IAM                   struct{}            `embed:"" prefix:"iam-"`
	kmsHandler            service.Registerable
	secretsManagerHandler service.Registerable
	ddbHandler            service.Registerable
	s3Handler             service.Registerable
	ssmHandler            service.Registerable
	iamHandler            service.Registerable
	stsHandler            service.Registerable
	snsHandler            service.Registerable
	sqsHandler            service.Registerable
	snsClient             *sns.Client
	iamClient             *iam.Client
	s3Client              *s3.Client
	ssmClient             *ssmsdk.Client
	ddbClient             *dynamodb.Client
	stsClient             *stssdk.Client
	sqsClient             *sqssdk.Client
	secretsManagerClient  *secretsmanager.Client
	kmsClient             *kms.Client
	Region                string              `                                  name:"region"     env:"REGION"     default:"us-east-1"    help:"AWS region."`                        //nolint:lll //config
	AccountID             string              `                                  name:"account-id" env:"ACCOUNT_ID" default:"000000000000" help:"Mock AWS account ID used in ARNs."`  //nolint:lll //config
	Port                  string              `                                  name:"port"       env:"PORT"       default:"8000"         help:"HTTP server port."`                  //nolint:lll //config
	LogLevel              string              `                                  name:"log-level"  env:"LOG_LEVEL"  default:"info"         help:"Log level (debug|info|warn|error)."` //nolint:lll //config
	S3                    s3backend.Settings  `embed:"" prefix:"s3-"`
	DynamoDB              ddbbackend.Settings `embed:"" prefix:"dynamodb-"`
	Demo                  bool                `                                  name:"demo"       env:"DEMO"       default:"false"        help:"Load demo data on startup."` //nolint:lll //config
}

// GetGlobalConfig returns the centralised account ID and region (config.Provider).
func (c *CLI) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{
		AccountID: c.AccountID,
		Region:    c.Region,
	}
}

// GetDynamoDBSettings returns DynamoDB settings (dynamodb.ConfigProvider).
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

// Run parses CLI / environment-variable configuration and starts Gopherstack.
// It is called from main() and exits on error.
func Run() {
	var cli CLI

	kong.Parse(
		&cli,
		kong.Name("gopherstack"),
		kong.Description("In-memory AWS DynamoDB + S3 compatible server."),
	)

	if err := run(context.Background(), cli); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run starts the server with the given CLI configuration.
// It is separated from Run so it can be exercised in tests without [os.Exit].
func run(ctx context.Context, cli CLI) error {
	log := buildLogger(cli.LogLevel)

	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfgVal, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion(cli.Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		awscfg.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.ErrorContext(ctx, "Failed to load AWS config", "error", err)

		return err
	}

	initializeClients(&cli, awsCfgVal)

	janitorCtx, janitorCancel := context.WithCancel(ctx)
	defer janitorCancel()

	appCtx := &service.AppContext{
		Logger:     log,
		Config:     &cli,
		JanitorCtx: janitorCtx,
	}

	services, err := initializeServices(appCtx)
	if err != nil {
		return err
	}

	e := echo.New()
	e.Pre(logger.EchoMiddleware(log))
	e.GET("/_gopherstack/health", healthHandler)

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	startBackgroundWorkers(janitorCtx, log, services)
	inMemMux.Handle("/", e)

	if cli.Demo {
		log.InfoContext(ctx, "Loading demo data...")
		err = demo.LoadData(ctx, log, &demo.Clients{
			DynamoDB:       cli.ddbClient,
			S3:             cli.s3Client,
			SQS:            cli.sqsClient,
			SNS:            cli.snsClient,
			IAM:            cli.iamClient,
			STS:            cli.stsClient,
			SSM:            cli.ssmClient,
			KMS:            cli.kmsClient,
			SecretsManager: cli.secretsManagerClient,
		})
		if err != nil {
			log.ErrorContext(ctx, "Failed to load demo data", "error", err)
		}
	}

	return startServer(ctx, log, cli.Port, e)
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
}

// initializeServices initializes all service providers.
func initializeServices(appCtx *service.AppContext) ([]service.Registerable, error) {
	var services []service.Registerable
	serviceProviders := []service.Provider{
		&ddbbackend.Provider{},
		&s3backend.Provider{},
		&ssmbackend.Provider{},
		&iambackend.Provider{},
		&stsbackend.Provider{},
		&snsbackend.Provider{},
		&sqsbackend.Provider{},
		&kmsbackend.Provider{},
		&secretsmanagerbackend.Provider{},
	}

	for _, provider := range serviceProviders {
		svc, err := provider.Init(appCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to init %s: %w", provider.Name(), err)
		}

		services = append(services, svc)
	}

	// Store handlers in CLI so dashboard can access them.
	if cli, ok := appCtx.Config.(*CLI); ok {
		cli.ddbHandler = services[0]
		cli.s3Handler = services[1]
		cli.ssmHandler = services[2]
		cli.iamHandler = services[3]
		cli.stsHandler = services[4]
		cli.snsHandler = services[5]
		cli.sqsHandler = services[6]
		cli.kmsHandler = services[7]
		cli.secretsManagerHandler = services[8]
	}

	// Wire SNS→SQS delivery: when SNS publishes a message, deliver it to SQS queues.
	wireSNSToSQS(services[5], services[6])

	// Init dashboard last so it can access all service handlers.
	dashSvc, err := (&dashboard.Provider{}).Init(appCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to init Dashboard: %w", err)
	}
	services = append(services, dashSvc)

	// The router sorts services by MatchPriority() at startup, so registration order
	// does not affect routing correctness.
	return services, nil
}

// startBackgroundWorkers starts all background workers from services.
func startBackgroundWorkers(ctx context.Context, log *slog.Logger, services []service.Registerable) {
	for _, svc := range services {
		if worker, ok := svc.(service.BackgroundWorker); ok {
			if workerErr := worker.StartWorker(ctx); workerErr != nil {
				log.ErrorContext(ctx, "failed to start background worker", "error", workerErr)
			}
		}
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

// startServer starts the HTTP server and blocks until it shuts down.
func startServer(ctx context.Context, log *slog.Logger, port string, e *echo.Echo) error {
	if port[0] != ':' {
		port = ":" + port
	}

	log.InfoContext(ctx, "Starting Gopherstack (DynamoDB + S3)", "port", port)
	log.InfoContext(ctx, "  DynamoDB endpoint", "url", "http://localhost"+port)
	log.InfoContext(ctx, "  S3 endpoint      ", "url", "http://localhost"+port+" (path-style)")
	log.InfoContext(ctx, "  Dashboard        ", "url", "http://localhost"+port+"/dashboard")

	server := &http.Server{
		Addr:         port,
		Handler:      e,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
		IdleTimeout:  defaultTimeout,
	}

	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

// buildLogger converts the CLI log-level string to a [slog.Logger].
func buildLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
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
	// Services lists all registered mock AWS services.
	Services []string `json:"services"`
}

// healthHandler returns a JSON status response for all mock services.
func healthHandler(c *echo.Context) error {
	return c.JSON(http.StatusOK, healthResponse{
		Status: "ok",
		Services: []string{
			"DynamoDB", "S3", "SSM", "IAM", "STS", "SNS", "SQS", "KMS", "SecretsManager",
		},
	})
}

func setupRegistry(
	e *echo.Echo,
	log *slog.Logger,
	services []service.Registerable,
) error {
	registry := service.NewRegistry(log)

	for _, svc := range services {
		if err := registry.Register(svc); err != nil {
			log.Error("Failed to register service", "service", svc.Name(), "error", err)

			return err
		}
	}

	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	return nil
}
