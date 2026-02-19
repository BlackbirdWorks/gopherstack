package config

import (
	"os"
	"time"
)

const (
	// DefaultPort is the default port for the application.
	DefaultPort = "8000"
	// DefaultTimeout is the default request timeout.
	DefaultTimeout = 120 * time.Second
)

// Config holds the application configuration.
type Config struct {
	Port    string
	Region  string
	Level   string
	Timeout time.Duration
	Demo    bool
}

// Load loads the configuration from environment variables.
func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = DefaultPort
	}

	return Config{
		Level:   os.Getenv("DEBUG"),
		Port:    port,
		Demo:    os.Getenv("DEMO") == "true",
		Region:  "us-east-1", // Default region
		Timeout: DefaultTimeout,
	}
}
