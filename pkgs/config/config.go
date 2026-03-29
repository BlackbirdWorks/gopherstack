// Package config provides centralized AWS configuration shared by all Gopherstack services.
package config

import (
	"sync"
	"time"
)

// GlobalConfig holds a reference to the shared AWS configuration state.
// It is thread-safe and can be updated at runtime.
type GlobalConfig struct {
	state *sharedState
}

// NewGlobalConfig creates a new GlobalConfig with the given initial state.
func NewGlobalConfig(accountID, region string, latencyMs int, janitorTimeout time.Duration, enforceIAM bool, autoPurgeTTL time.Duration) *GlobalConfig {
	return &GlobalConfig{
		state: &sharedState{
			AccountID:      accountID,
			Region:         region,
			LatencyMs:      latencyMs,
			JanitorTimeout: janitorTimeout,
			EnforceIAM:     enforceIAM,
			AutoPurgeTTL:   autoPurgeTTL,
		},
	}
}

// sharedState holds the actual configuration values.
type sharedState struct {
	mu             sync.RWMutex
	AccountID      string
	Region         string
	LatencyMs      int
	JanitorTimeout time.Duration
	EnforceIAM     bool
	AutoPurgeTTL   time.Duration
}

// GetAccountID returns the mock AWS account ID.
func (c *GlobalConfig) GetAccountID() string {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.AccountID
}

// GetRegion returns the default AWS region.
func (c *GlobalConfig) GetRegion() string {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.Region
}

// GetLatencyMs returns the maximum simulated response latency.
func (c *GlobalConfig) GetLatencyMs() int {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.LatencyMs
}

// GetJanitorTimeout returns the per-task janitor timeout.
func (c *GlobalConfig) GetJanitorTimeout() time.Duration {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.JanitorTimeout
}

// IsIAMEnforced returns true if IAM policy enforcement is enabled.
func (c *GlobalConfig) IsIAMEnforced() bool {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.EnforceIAM
}

// GetAutoPurgeTTL returns the auto-purge TTL duration.
func (c *GlobalConfig) GetAutoPurgeTTL() time.Duration {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.AutoPurgeTTL
}

// Update updates the configuration state with new values.
func (c *GlobalConfig) Update(accountID, region string, latencyMs int, janitorTimeout time.Duration, enforceIAM bool, autoPurgeTTL time.Duration) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.AccountID = accountID
	c.state.Region = region
	c.state.LatencyMs = latencyMs
	c.state.JanitorTimeout = janitorTimeout
	c.state.EnforceIAM = enforceIAM
	c.state.AutoPurgeTTL = autoPurgeTTL
}

// Provider is implemented by the CLI / any runtime configuration object
// that can supply a GlobalConfig pointer to services.
type Provider interface {
	GetGlobalConfig() *GlobalConfig
}
