package dashboard

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements the service.Provider interface for the Dashboard service.
type Provider struct{}

// Name returns the name of the service provider.
func (p *Provider) Name() string {
	return "Dashboard"
}

// Init initializes the Dashboard service.
// It extracts the fault store and config manager from the AppContext and
// returns a new DashboardHandler instance.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	// Extract the main config to get the fault store and other system components.
	// Since the orchestrator puts the *CLI or *TestStack etc into Config, we need
	// to ensure it satisfies what we need.
	cfg, ok := ctx.Config.(ConfigManager)
	if !ok {
		return nil, errors.New("app context config does not implement ConfigManager")
	}

	// We also need access to the fault store for the chaos dashboard.
	// In Gopherstack, the fault store is typically passed through the individual
	// service configs or global config.
	var faultStore *chaos.FaultStore
	var globalCfg *config.GlobalConfig

	// Try to extract fault store from known config types if they exist.
	// This is a safety measure to ensure we don't break different stack types.
	if c, hasFaultStore := ctx.Config.(interface{ GetFaultStore() *chaos.FaultStore }); hasFaultStore {
		faultStore = c.GetFaultStore()
	}
	if c, hasGlobalConfig := ctx.Config.(interface{ GetGlobalConfig() *config.GlobalConfig }); hasGlobalConfig {
		globalCfg = c.GetGlobalConfig()
	}

	h := NewHandler(Config{
		FaultStore:    faultStore,
		ConfigManager: cfg,
		GlobalConfig:  globalCfg,
		Logger:        ctx.Logger,
	})

	return h, nil
}
