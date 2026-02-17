package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

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

func main() {
	if err := startServer(); err != nil {
		os.Exit(1)
	}
}

func startServer() error {
	var level slog.Level
	if os.Getenv("DEBUG") == "true" {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	log := logger.NewLogger(level)

	// Create backends and handlers.
	ddbHandler := ddbbackend.NewHandler(log)
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend, log)

	// Wrap handlers with logger middleware to inject logger into request context
	loggerMiddleware := logger.Middleware(log)
	ddbHandlerWithLogger := loggerMiddleware(ddbHandler)
	s3HandlerWithLogger := loggerMiddleware(s3Handler)

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

	// Load demo data before creating the dashboard handler
	if os.Getenv("DEMO") == "true" {
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, ddbClient, s3Client); err != nil {
			log.Error("Failed to load demo data", "error", err)
		}
	}

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler, log)
	dashboardHandlerWithLogger := loggerMiddleware(dashboardHandler)

	// Create Echo app with routing
	e := echo.New()

	// Route DynamoDB requests via pre-middleware (matched by X-Amz-Target header, not path)
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			target := c.Request().Header.Get("X-Amz-Target")
			if len(target) >= 9 && target[:9] == "DynamoDB_" {
				ddbHandlerWithLogger.ServeHTTP(c.Response(), c.Request())

				return nil
			}

			return next(c)
		}
	})

	// Dashboard routes (strip /dashboard prefix for the handler)
	dashGroup := e.Group("/dashboard")
	dashGroup.Any("/*", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandlerWithLogger)))
	dashGroup.Any("", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandlerWithLogger)))

	// S3 catch-all (everything else)
	e.Any("/*", echo.WrapHandler(s3HandlerWithLogger))

	// Wire the in-memory mux used by SDK clients through the same Echo routing
	inMemMux.Handle("/", e)

	port := ":8000"
	log.Info("Starting Gopherstack (DynamoDB + S3)", "port", port)
	log.Info("  DynamoDB endpoint", "url", "http://localhost"+port)
	log.Info("  S3 endpoint      ", "url", "http://localhost"+port+" (path-style)")
	log.Info("  Dashboard        ", "url", "http://localhost"+port+"/dashboard")

	if err = e.Start(port); err != nil {
		log.Error("Failed to start server", "error", err)

		return err
	}

	return nil
}
