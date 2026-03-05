package opensearch

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// EngineConfig is the interface for accessing the OpenSearch engine mode configuration.
type EngineConfig interface {
	GetOpenSearchEngine() string
}

// Engine mode constants.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Provider implements service.Provider for the OpenSearch service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OpenSearch" }

// Init initializes the OpenSearch service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	engineMode := EngineStub

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	if ec, ok := ctx.Config.(EngineConfig); ok {
		if mode := ec.GetOpenSearchEngine(); mode != "" {
			engineMode = mode
		}
	}

	// docker mode is reserved for future use; for now both modes use the in-memory backend.
	_ = engineMode

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.Region = region

	return handler, nil
}
