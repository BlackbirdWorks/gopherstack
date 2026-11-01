package dms

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("DMS provider: nil AppContext")

// EngineModeConfig exposes the DMS engine mode (stub or docker).
type EngineModeConfig interface {
	GetDMSEngine() string
}

// EngineDocker is the engine mode that moves data for real.
const EngineDocker = "docker"

// Provider implements service.Provider for AWS DMS.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "DMS" }

// Init initializes the DMS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if ec, ok := ctx.Config.(EngineModeConfig); ok && ec.GetDMSEngine() == EngineDocker {
		backend.EnableEngine(EngineConfig{})
	}
	handler := NewHandler(backend)

	return handler, nil
}
