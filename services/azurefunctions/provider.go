package azurefunctions

import (
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider extracts the Azure Functions settings from the AppContext config.
type ConfigProvider interface {
	GetAzureFunctionsSettings() Settings
}

// Provider implements service.Provider for Azure Functions.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureFunctions" }

// Init builds the backend and handler; the port is bound by Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureFunctionsSettings()
	}

	backend := NewInMemoryBackend()
	backend.Configure(settings)

	if settings.Engine != EngineNone {
		lr := gcpserve.NewLazyRuntime(ctx.Logger, "azurefunctions.runtime")
		backend.SetRuntime(lr.Get)
		backend.SetOnClose(func() { _ = lr.Close() })
	}

	if ctx.PortAlloc != nil {
		backend.SetPortPool(ctx.PortAlloc)
	}

	return NewHandler(backend, settings), nil
}
