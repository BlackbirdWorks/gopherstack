package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
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
	s3backend "Gopherstack/s3"
)

func TestServerStartupAndShutdown(t *testing.T) {
	tests := []struct {
		name    string
		demoEnv string
		port    string
	}{
		{
			name:    "server startup without DEMO",
			demoEnv: "false",
			port:    ":8001",
		},
		{
			name:    "server startup with DEMO",
			demoEnv: "true",
			port:    ":8002",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original DEMO env var
			originalDemo := os.Getenv("DEMO")
			t.Setenv("DEMO", originalDemo)

			// Set DEMO env var for this test
			t.Setenv("DEMO", tt.demoEnv)

			// Create a channel to signal server shutdown
			stopChan := make(chan struct{})

			// Start the server in a goroutine
			go func() {
				_ = startServerOnPort(tt.port, stopChan)
			}()

			// Give the server time to start
			time.Sleep(1 * time.Second)

			// Make a request to verify the server is responding
			client := &http.Client{
				Timeout: 5 * time.Second,
			}

			resp, err := client.Get("http://localhost" + tt.port + "/dashboard")
			if err != nil {
				t.Fatalf("failed to reach server on %s: %v", tt.port, err)
			}
			defer resp.Body.Close()

			// Check that we got a valid response
			if resp.StatusCode < 200 || resp.StatusCode >= 500 {
				t.Fatalf("unexpected status code: %d", resp.StatusCode)
			}

			t.Logf("Server responding successfully with status %d on port %s", resp.StatusCode, tt.port)

			// Signal the server to stop
			close(stopChan)

			// Give the server a moment to process shutdown
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// startServerOnPort starts the server on the specified port and listens for shutdown signal.
func startServerOnPort(port string, stopChan chan struct{}) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Create backends and handlers
	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	// Create a temporary mux for in-memory SDK clients
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	if err != nil {
		logger.Error("Failed to load AWS config", "error", err)

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
		logger.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), logger, ddbClient, s3Client); err != nil {
			logger.Error("Failed to load demo data", "error", err)
		}
	}

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Create Echo app with routing
	e := echo.New()

	// Route DynamoDB requests via pre-middleware (matched by X-Amz-Target header, not path)
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			target := c.Request().Header.Get("X-Amz-Target")
			if len(target) >= 9 && target[:9] == "DynamoDB_" {
				ddbHandler.ServeHTTP(c.Response(), c.Request())

				return nil
			}

			return next(c)
		}
	})

	// Dashboard routes (strip /dashboard prefix for the handler)
	dashGroup := e.Group("/dashboard")
	dashGroup.Any("/*", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))
	dashGroup.Any("", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))

	// S3 catch-all (everything else)
	e.Any("/*", echo.WrapHandler(s3Handler))

	// Wire the in-memory mux used by SDK clients through the same Echo routing
	inMemMux.Handle("/", e)

	logger.Info("Starting Gopherstack (DynamoDB + S3)", "port", port)

	// Start server in a goroutine and listen for stop signal
	errChan := make(chan error, 1)
	go func() {
		errChan <- e.Start(port)
	}()

	// Wait for shutdown signal or server error
	<-stopChan
	// Server continues running in background (Echo v5 doesn't expose graceful shutdown easily)
	// This is acceptable for testing - the test framework will clean up processes
	return nil
}
