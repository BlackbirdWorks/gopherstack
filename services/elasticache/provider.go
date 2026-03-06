package elasticache

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// EngineConfig is the interface for accessing the ElastiCache engine mode configuration (embedded, stub, or docker).
type EngineConfig interface {
	GetElastiCacheEngine() string
}

// Provider implements service.Provider for the ElastiCache service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "ElastiCache" }

// Init initializes the ElastiCache service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	engineMode := EngineEmbedded
	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}
	if ec, ok := ctx.Config.(EngineConfig); ok {
		engineMode = ec.GetElastiCacheEngine()
	}

	backend := NewInMemoryBackend(engineMode, accountID, region)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.Region = region

	return handler, nil
}
