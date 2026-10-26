package gcpcompute

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpcompute: nil app context")

// ConfigProvider extracts Compute Engine configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPComputeSettings() Settings
}

// Provider implements service.Provider for Compute Engine. It serves the compute v1 REST API on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPCompute" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPComputeSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port

	return handler, nil
}
