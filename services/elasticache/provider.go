package elasticache

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var ErrNilContext = errors.New("nil context")

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
	if ctx == nil {
		return nil, ErrNilContext
	}
	engineMode := EngineEmbedded
	accountID, region := service.AccountRegionOrDefault(ctx)

	if ec, ok := ctx.Config.(EngineConfig); ok {
		engineMode = ec.GetElastiCacheEngine()
	}

	var allocator *portalloc.Allocator
	if ctx.PortAlloc != nil {
		allocator = ctx.PortAlloc
	}
	backend := NewInMemoryBackend(engineMode, accountID, region, allocator)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.Region = region

	return handler, nil
}
