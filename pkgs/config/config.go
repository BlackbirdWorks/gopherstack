// Package config provides centralized AWS configuration shared by all Gopherstack services.
package config

// GlobalConfig holds the centralized AWS account and region configuration
// injected into every service backend at construction time.
type GlobalConfig struct {
	// AccountID is the mock AWS account ID used in ARNs (default: "000000000000").
	AccountID string
	// Region is the default AWS region used when none can be extracted from a request.
	Region string
}

// Provider is implemented by the CLI / any runtime configuration object
// that can supply a GlobalConfig to services.
type Provider interface {
	GetGlobalConfig() GlobalConfig
}
