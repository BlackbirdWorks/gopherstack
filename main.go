package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
)

func main() {
	cfg := config.Load()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)

		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	// Parse log level from config
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	case "info":
		fallthrough
	default:
		level = slog.LevelInfo
	}

	log := logger.NewLogger(level)

	// Create backends and handlers.
	ddbBackend := ddbbackend.NewInMemoryDB()
	ddbHandler := ddbbackend.NewHandler(ddbBackend, log)
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend, log)
	// Set endpoint to support virtual-hosted-style requests
	s3Port := strings.TrimPrefix(cfg.Port, ":")
	s3Handler.Endpoint = "localhost:" + s3Port

	// Start background janitors for async deletion.
	// The context is cancelled when run() returns (on server shutdown or error).
	janitorCtx, janitorCancel := context.WithCancel(context.Background())
	defer janitorCancel()

	go s3backend.NewJanitor(s3Backend, log).Run(janitorCtx)
	go ddbbackend.NewJanitor(ddbBackend, log).Run(janitorCtx)

	// Create a temporary mux for in-memory SDK clients
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	awsCfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion(cfg.Region),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
		awscfg.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.Error("Failed to load AWS config", "error", err)

		return err
	}

	// Both SDK clients point to the same "http://local" endpoint
	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler, log)

	// Create Echo app with routing
	e := echo.New()
	e.Pre(logger.EchoMiddleware(log))

	// Register all services in priority order
	services := []service.Registerable{
		ddbHandler,       // Priority 100 (header-based)
		dashboardHandler, // Priority 50 (path-based)
		s3Handler,        // Priority 0 (catch-all)
	}

	if setupErr := setupRegistry(e, log, services); setupErr != nil {
		return setupErr
	}

	// Wire the in-memory mux used by SDK clients through the same Echo routing
	inMemMux.Handle("/", e)

	// Load demo data once the server is ready (internally)
	if cfg.Demo {
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, ddbClient, s3Client); err != nil {
			log.Error("Failed to load demo data", "error", err)
		}
	}

	port := cfg.Port
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
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
		IdleTimeout:  cfg.Timeout,
	}

	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("Failed to start server", "error", err)

		return err
	}

	return nil
}

func setupRegistry(
	e *echo.Echo,
	log *slog.Logger,
	services []service.Registerable,
) error {
	registry := service.NewRegistry(log)

	// Register all services
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
