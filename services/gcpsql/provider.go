package gcpsql

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpsql: nil app context")

// ConfigProvider extracts Cloud SQL configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPSQLSettings() Settings
}

// Provider implements service.Provider for Google Cloud SQL. It serves the Admin REST API on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPSQL" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPSQLSettings()
	}

	backend := NewInMemoryBackend()
	backend.SetStartupTimeout(settings.StartupTimeout)

	if settings.Engine != EngineNone {
		backend.SetLauncher(newDockerLauncher(ctx.Logger, ctx.PortAlloc, settings.StartupTimeout))
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
