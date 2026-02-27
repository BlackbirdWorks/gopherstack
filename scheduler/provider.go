package scheduler

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for EventBridge Scheduler.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Scheduler" }

// Init initializes the Scheduler service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := "000000000000"
	region := "us-east-1"

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
