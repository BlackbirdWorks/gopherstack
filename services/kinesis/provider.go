package kinesis

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Kinesis service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "Kinesis" }

// Init initializes the Kinesis service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend
	var defaultRegion, accountID string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.AccountID, cfg.Region)
		defaultRegion = cfg.Region
		accountID = cfg.AccountID
	} else {
		backend = NewInMemoryBackend()
	}

	handler := NewHandler(backend)
	handler.DefaultRegion = defaultRegion
	handler.AccountID = accountID

	return handler, nil
}
