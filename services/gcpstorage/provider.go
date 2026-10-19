package gcpstorage

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpstorage: nil app context")

// ConfigProvider extracts GCS configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPStorageSettings() Settings
}

// Provider implements service.Provider for Google Cloud Storage. Like the
// Azure services it runs on its own listener (see Handler.StartWorker) and
// never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPStorage" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPStorageSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port

	return handler, nil
}
