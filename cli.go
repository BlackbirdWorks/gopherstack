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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
)

const (
	defaultPort    = "8000"
	defaultRegion  = "us-east-1"
	defaultTimeout = 120 * time.Second
)

// CLI holds all command-line / environment-variable configuration for Gopherstack.
type CLI struct {
	// LogLevel is the log level: debug, info, warn, error.
	LogLevel string `name:"log-level" env:"LOG_LEVEL" default:"info" help:"Log level (debug|info|warn|error)."`
	// Port is the HTTP server port.
	Port string `name:"port" env:"PORT" default:"8000" help:"HTTP server port."`
	// Region is the AWS region used for SDK clients.
	Region string `name:"region" env:"REGION" default:"us-east-1" help:"AWS region."`
	// Demo enables loading of demo data on startup.
	Demo bool `name:"demo" env:"DEMO" default:"false" help:"Load demo data on startup."`

	// DynamoDB holds DynamoDB service-level settings.
	DynamoDB ddbbackend.Settings `embed:"" prefix:"dynamodb-"`
	// S3 holds S3 service-level settings.
	S3 s3backend.Settings `embed:"" prefix:"s3-"`
	// SSM holds SSM service-level settings.
	SSM struct{} `embed:"" prefix:"ssm-"`

	ddbClient *dynamodb.Client
	s3Client  *s3.Client
	ssmClient *ssmsdk.Client

	ddbHandler service.Registerable
	s3Handler  service.Registerable
	ssmHandler service.Registerable
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

// GetDynamoDBHandler returns the DynamoDB handler (dashboard.AWSSDKProvider).
func (c *CLI) GetDynamoDBHandler() service.Registerable { return c.ddbHandler }

// GetS3Handler returns the S3 handler (dashboard.AWSSDKProvider).
func (c *CLI) GetS3Handler() service.Registerable { return c.s3Handler }

// GetSSMHandler returns the SSM handler (dashboard.AWSSDKProvider).
func (c *CLI) GetSSMHandler() service.Registerable { return c.ssmHandler }

// Run parses CLI / environment-variable configuration and starts Gopherstack.
// It is called from main() and exits on error.
func Run() {
	var cli CLI

	kong.Parse(
		&cli,
		kong.Name("gopherstack"),
		kong.Description("In-memory AWS DynamoDB + S3 compatible server."),
	)

	if err := run(cli); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run starts the server with the given CLI configuration.
// It is separated from Run so it can be exercised in tests without [os.Exit].
func run(cli CLI) error {
	log := buildLogger(cli.LogLevel)

	// In-memory SDK routing for dashboard
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfgVal, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion(cli.Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		awscfg.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.Error("Failed to load AWS config", "error", err)
		return err
	}

	cli.ddbClient = dynamodb.NewFromConfig(awsCfgVal, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String("http://local") })
	cli.s3Client = s3.NewFromConfig(awsCfgVal, func(o *s3.Options) { o.UsePathStyle = true; o.BaseEndpoint = aws.String("http://local") })
	cli.ssmClient = ssmsdk.NewFromConfig(awsCfgVal, func(o *ssmsdk.Options) { o.BaseEndpoint = aws.String("http://local") })

	// The context is cancelled when run() returns (server shutdown or error).
	janitorCtx, janitorCancel := context.WithCancel(context.Background())
	defer janitorCancel()

	// 1. AppContext holds the config object that implements the extraction interfaces
	appCtx := &service.AppContext{
		Logger:     log,
		Config:     &cli,
		JanitorCtx: janitorCtx,
	}

	// 2. Initialize Core Storage Backend Providers first to make them available for the dashboard
	ddbProvider := &ddbbackend.Provider{}
	s3Provider := &s3backend.Provider{}
	ssmProvider := &ssmbackend.Provider{}

	ddbSvc, err := ddbProvider.Init(appCtx)
	if err != nil {
		return fmt.Errorf("failed to init %s: %w", ddbProvider.Name(), err)
	}
	cli.ddbHandler = ddbSvc

	s3Svc, err := s3Provider.Init(appCtx)
	if err != nil {
		return fmt.Errorf("failed to init %s: %w", s3Provider.Name(), err)
	}
	cli.s3Handler = s3Svc

	ssmSvc, err := ssmProvider.Init(appCtx)
	if err != nil {
		return fmt.Errorf("failed to init %s: %w", ssmProvider.Name(), err)
	}
	cli.ssmHandler = ssmSvc

	// 3. Initialize Dashboard Provider (which will use the Config extractors to get handlers)
	dashboardProvider := &dashboard.Provider{}
	dashboardSvc, err := dashboardProvider.Init(appCtx)
	if err != nil {
		return fmt.Errorf("failed to init %s: %w", dashboardProvider.Name(), err)
	}

	// Create Echo app with routing.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(log))

	services := []service.Registerable{
		ddbSvc,       // Priority 100 (header-based)
		ssmSvc,       // Priority 100 (header-based)
		dashboardSvc, // Priority 50 (path-based)
		s3Svc,        // Priority 0 (catch-all)
	}

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	// Start background workers dynamically
	for _, svc := range services {
		if worker, ok := svc.(service.BackgroundWorker); ok {
			go worker.StartWorker(janitorCtx)
		}
	}

	// Wire the in-memory mux through the same Echo routing.
	inMemMux.Handle("/", e)

	// Load demo data once routing is wired.
	if cli.Demo {
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, cli.ddbClient, cli.s3Client); err != nil {
			log.Error("Failed to load demo data", "error", err)
		}
	}

	port := cli.Port
	if port[0] != ':' {
		port = ":" + port
	}
	log.Info("Starting Gopherstack (DynamoDB + S3)", "port", port)
	log.Info("  DynamoDB endpoint", "url", "http://localhost"+port)
	log.Info("  S3 endpoint      ", "url", "http://localhost"+port+" (path-style)")
	log.Info("  Dashboard        ", "url", "http://localhost"+port+"/dashboard")

	server := &http.Server{
		Addr:         port,
		Handler:      e,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
		IdleTimeout:  defaultTimeout,
	}

	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("Failed to start server", "error", err)

		return err
	}

	return nil
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
