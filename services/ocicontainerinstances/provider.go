package ocicontainerinstances

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocicontainerinstances: nil app context")

// ConfigProvider extracts OCI Container Instances configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIContainerInstancesSettings() Settings
}

// Provider implements service.Provider for OCI Container Instances. It runs on its own listener
// (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIContainerInstances" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIContainerInstancesSettings()
	}

	backend := NewInMemoryBackend()

	if settings.Engine == EngineDocker {
		lr := gcpserve.NewLazyRuntime(ctx.Logger, "ocicontainerinstances.runtime")
		eng := NewEngine(func() (Runtime, error) {
			r, err := lr.Get()
			if err != nil {
				return nil, err
			}

			return NewDockerRuntime(r), nil
		})
		eng.onClose = func() { _ = lr.Close() }
		backend.SetEngine(eng)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
