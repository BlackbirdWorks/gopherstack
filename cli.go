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
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
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
}

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

	// Create backends and handlers.
	ddbBackend := ddbbackend.NewInMemoryDB()
	ddbHandler := ddbbackend.NewHandler(ddbBackend, log)
	s3Bk := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Bk, log)

	// Set endpoint to support virtual-hosted-style requests.
	s3Port := strings.TrimPrefix(cli.Port, ":")
	s3Handler.Endpoint = "localhost:" + s3Port

	// Start background janitors for async deletion.
	// The context is cancelled when run() returns (server shutdown or error).
	janitorCtx, janitorCancel := context.WithCancel(context.Background())
	defer janitorCancel()

	go s3backend.NewJanitor(s3Bk, log, cli.S3).Run(janitorCtx)
	go ddbbackend.NewJanitor(ddbBackend, log, cli.DynamoDB).Run(janitorCtx)

	// Create a mux for in-memory SDK clients used by the dashboard.
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfgVal, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion(cli.Region),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
		awscfg.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.Error("Failed to load AWS config", "error", err)

		return err
	}

	// Both SDK clients point to the same "http://local" endpoint.
	ddbClient := dynamodb.NewFromConfig(awsCfgVal, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(awsCfgVal, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler, log)

	// Create Echo app with routing.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(log))

	services := []service.Registerable{
		ddbHandler,       // Priority 100 (header-based)
		dashboardHandler, // Priority 50 (path-based)
		s3Handler,        // Priority 0 (catch-all)
	}

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	// Wire the in-memory mux through the same Echo routing.
	inMemMux.Handle("/", e)

	// Load demo data once routing is wired.
	if cli.Demo {
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, ddbClient, s3Client); err != nil {
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
