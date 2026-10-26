package ocifunctions

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocifunctions: nil app context")

// ConfigProvider extracts OCI Functions configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIFunctionsSettings() Settings
}

// Provider implements service.Provider for OCI Functions. It runs on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIFunctions" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIFunctionsSettings()
	}

	backend := NewInMemoryBackend()
	backend.Configure(settings)

	if !settings.DisableEngine {
		lr := gcpserve.NewLazyRuntime(ctx.Logger, "ocifunctions.runtime")
		backend.SetRuntime(func() (Runner, error) { return lr.Get() })
		backend.SetOnClose(func() { _ = lr.Close() })
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
