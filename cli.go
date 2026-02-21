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
	defaultPort     = "8000"
	defaultRegion   = "us-east-1"
	defaultTimeout  = 30 * time.Second
	shutdownTimeout = 5 * time.Second
)

// CLI holds all command-line / environment-variable configuration for Gopherstack.
type CLI struct {
	ddbClient  *dynamodb.Client
	s3Client   *s3.Client
	ssmClient  *ssmsdk.Client
	ddbHandler service.Registerable
	s3Handler  service.Registerable
	ssmHandler service.Registerable

	// LogLevel is the log level: debug, info, warn, error.
	LogLevel string `name:"log-level" env:"LOG_LEVEL" default:"info" help:"Log level (debug|info|warn|error)."`
	// Port is the HTTP server port.
	Port string `name:"port" env:"PORT" default:"8000" help:"HTTP server port."`
	// Region is the AWS region used for SDK clients.
	Region string `name:"region" env:"REGION" default:"us-east-1" help:"AWS region."`

	// DynamoDB holds DynamoDB service-level settings.
	DynamoDB ddbbackend.Settings `embed:"" prefix:"dynamodb-"`
	// S3 holds S3 service-level settings.
	S3 s3backend.Settings `embed:"" prefix:"s3-"`
	// SSM holds SSM service-level settings.
	SSM struct{} `embed:"" prefix:"ssm-"`

	// Demo enables loading of demo data on startup.
	Demo bool `name:"demo" env:"DEMO" default:"false" help:"Load demo data on startup."`
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

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	startBackgroundWorkers(janitorCtx, log, services)
	inMemMux.Handle("/", e)

	if cli.Demo {
		log.InfoContext(ctx, "Loading demo data...")
		if err = demo.LoadData(ctx, log, cli.ddbClient, cli.s3Client); err != nil {
			log.ErrorContext(ctx, "Failed to load demo data", "error", err)
		}
	}

	return startServer(ctx, log, cli.Port, e)
}

// initializeClients configures the AWS SDK clients for DynamoDB, S3, and SSM.
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
}

// initializeServices initializes all service providers.
func initializeServices(appCtx *service.AppContext) ([]service.Registerable, error) {
	var services []service.Registerable
	providers := []service.Provider{
		&ddbbackend.Provider{},
		&s3backend.Provider{},
		&ssmbackend.Provider{},
		&dashboard.Provider{},
	}

	for _, provider := range providers {
		svc, err := provider.Init(appCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to init %s: %w", provider.Name(), err)
		}
		services = append(services, svc)
	}

	// Reorder for routing priority: DynamoDB (100), SSM (100), Dashboard (50), S3 (0)
	return []service.Registerable{services[0], services[2], services[3], services[1]}, nil
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
