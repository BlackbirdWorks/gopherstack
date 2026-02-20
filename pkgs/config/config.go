package config

import (
	"os"
	"strings"
	"time"
)

const (
	// DefaultPort is the default port for the application.
	DefaultPort = "8000"
	// DefaultTimeout is the default request timeout.
	DefaultTimeout = 120 * time.Second
	// DefaultRegion is the default AWS region.
	DefaultRegion = "us-east-1"
	// DefaultLogLevel is the default log level.
	DefaultLogLevel = "info"
)

// Config holds the application configuration.
type Config struct {
	// Port is the HTTP server port (default: 8000).
	Port string
	// Region is the AWS region (default: us-east-1).
	Region string
	// Level is the log level: debug, info, warn, error (default: info).
	Level string
	// Timeout is the HTTP request timeout (default: 120s).
	Timeout time.Duration
	// Demo enables loading demo data on startup.
	Demo bool
}

// Load loads the configuration from environment variables.
// Supports:
//   - PORT: HTTP server port (default: 8000)
//   - REGION: AWS region (default: us-east-1)
//   - LOG_LEVEL: Log level - debug, info, warn, error (default: info)
//   - DEBUG: Deprecated - use LOG_LEVEL=debug instead
//   - DEMO: Enable demo data (true/false)
func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = DefaultPort
	}

	// Support both LOG_LEVEL and DEBUG environment variables
	// LOG_LEVEL takes precedence
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		// Fallback to DEBUG for backward compatibility
		if os.Getenv("DEBUG") == "true" {
			logLevel = "debug"
		} else {
			logLevel = DefaultLogLevel
		}
	}
	logLevel = strings.ToLower(strings.TrimSpace(logLevel))

	region := os.Getenv("REGION")
	if region == "" {
		region = DefaultRegion
	}

	return Config{
		Level:   logLevel,
		Port:    port,
		Region:  region,
		Demo:    os.Getenv("DEMO") == "true",
		Timeout: DefaultTimeout,
	}
}
