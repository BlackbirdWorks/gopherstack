package gcpcloudbuild

import (
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider extracts Cloud Build configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPCloudBuildSettings() Settings
}

// Provider implements service.Provider for Google Cloud Build. It serves gRPC and REST on its own listener
// (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPCloudBuild" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPCloudBuildSettings()
	}

	backend := NewInMemoryBackend()

	if ctx.JanitorCtx != nil {
		backend.SetContext(ctx.JanitorCtx)
	}

	enableEngine(ctx, backend, settings)

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}

func enableEngine(ctx *service.AppContext, backend *InMemoryBackend, settings Settings) {
	if settings.Engine == EngineNone {
		backend.DisableEngine(reasonDisabled)

		return
	}

	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn(
			"gcpcloudbuild: container runtime unavailable; builds will fail with a status detail",
			"error",
			err,
		)

		return
	}

	backend.SetRuntime(rt)
}
