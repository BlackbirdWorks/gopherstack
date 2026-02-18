package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"

	"Gopherstack/dashboard"
	"Gopherstack/demo"
	ddbbackend "Gopherstack/dynamodb"
	"Gopherstack/pkgs/logger"
	s3backend "Gopherstack/s3"
)

const (
	serverTimeout = 120 * time.Second
	trueStr       = "true"
)

func main() {
	if err := startServer(); err != nil {
		os.Exit(1)
	}
}

func startServer() error {
	var level slog.Level
	if os.Getenv("DEBUG") == trueStr {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	log := logger.NewLogger(level)

	// Create backends and handlers.
	ddbHandler := ddbbackend.NewHandler(log)
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend, log)

	// Create a temporary mux for in-memory SDK clients
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	if err != nil {
		log.Error("Failed to load AWS config", "error", err)

		return err
	}

	// Both SDK clients point to the same "http://local" endpoint
	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler, log)

	// Create Echo app with routing
	e := echo.New()

	// Add logger middleware as Pre middleware so it is available to other Pre middlewares
	// and routes that intercept the request early (like DynamoDB).
	e.Pre(logger.EchoMiddleware(log))

	// Route DynamoDB requests via pre-middleware (matched by X-Amz-Target header, not path)
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			target := c.Request().Header.Get("X-Amz-Target")
			if len(target) >= 9 && target[:9] == "DynamoDB_" {
				return ddbHandler.Handle(c)
			}

			return next(c)
		}
	})

	// Dashboard routes
	dashGroup := e.Group("/dashboard")
	dashGroup.Any("/*", dashboardHandler.Handle)
	dashGroup.Any("", dashboardHandler.Handle)

	// Metrics endpoints
	dashboard.RegisterMetricsHandlers(e)

	// S3 catch-all (everything else)
	e.Any("/*", s3Handler.Handle)

	// Wire the in-memory mux used by SDK clients through the same Echo routing
	inMemMux.Handle("/", e)

	// Load demo data once the server is ready (internally)
	if os.Getenv("DEMO") == trueStr {
		// Use a separate goroutine or just call it here.
		// Since ListenAndServe hasn't started yet but handlers are wired, it's safe.
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, ddbClient, s3Client); err != nil {
			log.Error("Failed to load demo data", "error", err)
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
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
		ReadTimeout:  serverTimeout,
		WriteTimeout: serverTimeout,
		IdleTimeout:  serverTimeout,
	}

	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("Failed to start server", "error", err)

		return err
	}

	return nil
}
