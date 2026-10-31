package gcpdatastore

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpdatastore: nil app context")

// ConfigProvider extracts Datastore configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPDatastoreSettings() Settings
}

// Provider implements service.Provider for Google Cloud Datastore. It serves gRPC and REST on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPDatastore" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPDatastoreSettings()
	}

	backend := NewInMemoryBackend()
	backend.SetEnforceIndexes(settings.EnforceIndexes)

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
