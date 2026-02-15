package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"Gopherstack/dashboard"
	"Gopherstack/demo"
	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
	idleTimeout  = 120 * time.Second
)

// newMixedHandler returns an [http.Handler] that routes requests to either the
// DynamoDB or S3 handler based on the X-Amz-Target header, and to the
// dashboard handler for /dashboard paths.
//
// DynamoDB SDK always sets X-Amz-Target: DynamoDB_20120810.<Operation>.
// S3 SDK never sets this header, so every other request goes to S3.
func newMixedHandler(ddb, s3api, dash http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/dashboard") {
			http.StripPrefix("/dashboard", dash).ServeHTTP(w, r)

			return
		}

		if strings.HasPrefix(r.Header.Get("X-Amz-Target"), "DynamoDB_") {
			ddb.ServeHTTP(w, r)

			return
		}

		s3api.ServeHTTP(w, r)
	})
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Create backends and handlers.
	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	// Create an in-memory HTTP client for the dashboard's internal SDK clients.
	// We use a temporary mixed handler without the dashboard (dashboard hasn't
	// been created yet) and attach the dashboard later.
	inMemMux := http.NewServeMux()
	inMemClient := &dashboard.InMemClient{Handler: inMemMux}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	if err != nil {
		logger.Error("Failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Both SDK clients point to the same "http://local" endpoint.
	// The mixed handler routes them correctly by X-Amz-Target header.
	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})

	// Load demo data before creating the dashboard handler.
	if os.Getenv("DEMO") == "true" {
		logger.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), logger, ddbClient, s3Client); err != nil {
			logger.Error("Failed to load demo data", "error", err)
		}
	}

	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Wire the in-memory mux used by the internal SDK clients.
	mixed := newMixedHandler(ddbHandler, s3Handler, dashboardHandler)
	inMemMux.Handle("/", mixed)

	// The public server uses the same mixed handler.
	port := ":8000"
	logger.Info("Starting Gopherstack (DynamoDB + S3)", "port", port)
	logger.Info("  DynamoDB endpoint", "url", "http://localhost"+port)
	logger.Info("  S3 endpoint      ", "url", "http://localhost"+port+" (path-style)")
	logger.Info("  Dashboard        ", "url", "http://localhost"+port+"/dashboard")

	srv := &http.Server{
		Addr:         port,
		Handler:      mixed,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	if err = srv.ListenAndServe(); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
