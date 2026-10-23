package gcprun

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcprun: nil app context")

// ConfigProvider extracts Cloud Run configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPRunSettings() Settings
}

// Provider implements service.Provider for Google Cloud Run. It serves gRPC on its own listener
// (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPRun" }

// Init creates the backend and handler; the ports are bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPRunSettings()
	}

	backend := NewInMemoryBackend()
	backend.SetStartupTimeout(settings.StartupTimeout)

	if settings.Engine != EngineNone {
		lr := newLazyRuntime(ctx)
		backend.SetRuntime(lr.get)
		backend.SetOnClose(func() { _ = lr.Close() })
	}

	if ctx.PortAlloc != nil {
		backend.SetPortPool(ctx.PortAlloc)
	}

	handler := NewHandler(backend)
	handler.Port, handler.IngressPort = settings.Port, settings.IngressPort

	return handler, nil
}
