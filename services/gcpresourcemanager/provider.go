package gcpresourcemanager

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpresourcemanager: nil app context")

// ConfigProvider extracts Resource Manager configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPResourceManagerSettings() Settings
}

// EnforceConfigProvider extracts the opt-in GCP enforcement switches from the AppContext Config.
type EnforceConfigProvider interface {
	GetGCPEnforceSettings() gcpenforce.Settings
}

// Provider implements service.Provider for Resource Manager and Service Usage. It serves gRPC and REST on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPResourceManager" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPResourceManagerSettings()
	}

	handler := NewHandler(NewBackend(settings))
	handler.Port = settings.Port

	if ep, ok := ctx.Config.(EnforceConfigProvider); ok {
		handler.SetEnforcement(ep.GetGCPEnforceSettings())
	}

	return handler, nil
}
