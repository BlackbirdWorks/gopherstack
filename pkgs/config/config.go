// Package config provides centralized AWS configuration shared by all Gopherstack services.
package config

import "time"

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
	// JanitorTimeout is the per-task timeout applied to individual janitor operations
	// (TTL sweeps, table cleaners, stream sweepers, etc.). A context derived from the
	// janitor's parent context is bounded by this duration so that a stalled operation
	// cannot block the janitor loop indefinitely. Zero disables per-task timeouts.
	JanitorTimeout time.Duration
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
