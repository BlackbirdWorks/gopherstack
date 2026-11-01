package ocifilestorage

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocifilestorage: nil app context")

// ConfigProvider extracts OCI File Storage configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIFileStorageSettings() Settings
}

// EngineConfigProvider exposes the effective engine mode (stub or nfs).
type EngineConfigProvider interface {
	GetOCIFileStorageEngine() string
}

// Provider implements service.Provider for OCI File Storage. It runs on its own listener (see Handler.StartWorker)
// and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIFileStorage" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIFileStorageSettings()
	}

	engine := settings.Engine
	if ec, ok := ctx.Config.(EngineConfigProvider); ok {
		engine = ec.GetOCIFileStorageEngine()
	}

	backend := NewInMemoryBackend()

	if engine == EngineNFS {
		plane, err := NewNFSPlane(NFSConfig{Ports: ctx.PortAlloc, Dir: settings.DataDir})
		if err != nil {
			return nil, err
		}

		backend.SetDataPlane(plane)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
