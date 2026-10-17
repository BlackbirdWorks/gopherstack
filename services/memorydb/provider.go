package memorydb

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to MemoryDB Provider.Init")

// EngineConfig exposes the MemoryDB engine mode (embedded, stub, or docker).
type EngineConfig interface {
	GetMemoryDBEngine() string
}

// Provider implements service.Provider for the MemoryDB service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MemoryDB" }

// Init initializes the MemoryDB service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := newInMemoryBackendWithDefaults(region, accountID)
	engineMode := EngineEmbedded
	if ec, ok := ctx.Config.(EngineConfig); ok {
		engineMode = ec.GetMemoryDBEngine()
	}

	backend.SetEngine(engineMode, ctx.PortAlloc)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
