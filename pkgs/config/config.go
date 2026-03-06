// Package config provides centralized AWS configuration shared by all Gopherstack services.
package config

// GlobalConfig holds the centralized AWS account and region configuration
// injected into every service backend at construction time.
type GlobalConfig struct {
	// AccountID is the mock AWS account ID used in ARNs (default: "000000000000").
	AccountID string
	// Region is the default AWS region used when none can be extracted from a request.
	Region string
	// LatencyMs is the maximum simulated response latency in milliseconds.
	// Each request sleeps for a random duration in [0, LatencyMs). Zero disables latency simulation.
	LatencyMs int
	// EnforceIAM enables IAM policy enforcement when true.
	// When false (default), all requests are allowed regardless of policies.
	// When true, every incoming AWS API request is evaluated against attached IAM policies.
	EnforceIAM bool
}

// Provider is implemented by the CLI / any runtime configuration object
// that can supply a GlobalConfig to services.
type Provider interface {
	GetGlobalConfig() GlobalConfig
}
