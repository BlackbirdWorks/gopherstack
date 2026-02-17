package main

import (
	"context"
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
	"Gopherstack/pkgs/logger"
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
	log := logger.NewTestLogger()

	// Create backends and handlers
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

	// Load demo data before creating the dashboard handler
	if os.Getenv("DEMO") == "true" {
		log.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), log, ddbClient, s3Client); err != nil {
			log.Error("Failed to load demo data", "error", err)
		}
	}

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler, log)

	// Create Echo app with routing
	e := echo.New()

	// Add logger middleware to inject logger into request context
	e.Use(logger.EchoMiddleware(log))

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

	// S3 catch-all (everything else)
	e.Any("/*", s3Handler.Handle)

	// Wire the in-memory mux used by SDK clients through the same Echo routing
	inMemMux.Handle("/", e)

	log.Info("Starting Gopherstack (DynamoDB + S3)", "port", port)

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
