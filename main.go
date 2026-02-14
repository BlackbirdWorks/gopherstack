package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"Gopherstack/dynamodb"
	"Gopherstack/s3"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
	idleTimeout  = 120 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	mux := http.NewServeMux()
	mux.Handle("/", dynamodb.NewHandler())

	s3Backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	s3Handler := s3.NewHandler(s3Backend)
	mux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	port := ":8000"
	logger.Info("Starting Gopherstack (DynamoDB & S3 local)", "port", port)

	srv := &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
