package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Create Backends
	ddbHandler := ddbbackend.NewHandler()

	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	// Create a multiplexer for the API
	// This mux will be used by the InMemClient to route SDK requests directly to handlers
	apiMux := http.NewServeMux()
	apiMux.Handle("/", ddbHandler)
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Handler))  // Handle /s3 without trailing slash
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler)) // Handle /s3/ tree

	// Create In-Memory Client
	inMemClient := &dashboard.InMemClient{Handler: apiMux}

	// Load AWS Config with In-Memory Client
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	if err != nil {
		logger.Error("Failed to load AWS config", "error", err)
		os.Exit(1)
	}

	// Create SDK Clients
	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Important for our simple S3 handler
		o.BaseEndpoint = aws.String("http://local/s3")
	})

	// Load Demo Data
	if os.Getenv("DEMO") == "true" {
		logger.Info("Loading demo data...")
		if err = demo.LoadData(context.Background(), logger, ddbClient, s3Client); err != nil {
			logger.Error("Failed to load demo data", "error", err)
		}
	}

	// Create Dashboard Handler
	dashboardHandler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Main Server Mux
	// We mount the API mux and the Dashboard handler
	rootMux := http.NewServeMux()

	// Mount API handlers
	// Note: We need to replicate the routing logic from apiMux or just use apiMux as base
	// Since apiMux handles / and /s3/, we can just register those on rootMux too
	// But we need to be careful about not shadowing /dashboard

	rootMux.Handle("/", ddbHandler)
	rootMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	// Mount Dashboard
	rootMux.Handle("/dashboard/", http.StripPrefix("/dashboard", dashboardHandler))

	port := ":8000"
	logger.Info("Starting Gopherstack (DynamoDB & S3 local)", "port", port)
	logger.Info("Dashboard available", "url", "http://localhost"+port+"/dashboard")

	srv := &http.Server{
		Addr:         port,
		Handler:      rootMux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	if err = srv.ListenAndServe(); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
