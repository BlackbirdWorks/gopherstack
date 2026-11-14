package ocidatabase

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocidatabase: nil app context")

// ConfigProvider extracts OCI Database configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIDatabaseSettings() Settings
}

// EngineConfigProvider exposes the effective engine mode (stub or docker).
type EngineConfigProvider interface {
	GetOCIDatabaseEngine() string
}

// Provider implements service.Provider for OCI Database. It runs on its own listener (see Handler.StartWorker)
// and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIDatabase" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIDatabaseSettings()
	}

	engine := settings.Engine
	if ec, ok := ctx.Config.(EngineConfigProvider); ok {
		engine = ec.GetOCIDatabaseEngine()
	}

	backend := NewInMemoryBackend()

	if engine == EngineDocker {
		lr := gcpserve.NewLazyRuntime(ctx.Logger, "ocidatabase.runtime")
		image := os.Getenv("OCI_DATABASE_IMAGE")

		if image == "" {
			image = DefaultImage
		}

		backend.SetLauncher(NewDockerLauncher(DockerConfig{Runtime: lr.Get, Ports: ctx.PortAlloc}), image)
		backend.SetCloser(func() { _ = lr.Close() })
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
