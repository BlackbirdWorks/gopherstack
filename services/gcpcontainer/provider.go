package gcpcontainer

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpcontainer: nil app context")

// ConfigProvider extracts Kubernetes Engine configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPContainerSettings() Settings
}

// EngineConfigProvider exposes the engine mode (stub or docker).
type EngineConfigProvider interface {
	GetGCPContainerEngine() string
}

// Provider implements service.Provider for Kubernetes Engine. It serves gRPC and REST on its own listener
// (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPContainer" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPContainerSettings()
	}

	backend := NewInMemoryBackend()
	if ec, ok := ctx.Config.(EngineConfigProvider); ok && ec.GetGCPContainerEngine() == EngineDocker {
		enableDockerEngine(ctx, backend)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("GKE: container runtime unavailable; clusters stay metadata-only", "error", err)

		return
	}

	err = backend.EnableEngine(EngineConfig{
		Runtime: rt,
		Ports:   ctx.PortAlloc,
		Logger:  ctx.Logger,
		Host:    os.Getenv("GKE_CLUSTER_HOST"),
		Token:   os.Getenv("GKE_CLUSTER_TOKEN"),
		Image:   os.Getenv("GKE_K3S_IMAGE"),
	})
	if err != nil {
		ctx.Logger.Warn("GKE: invalid cluster engine configuration; clusters stay metadata-only", "error", err)
		_ = rt.Close()
	}
}
