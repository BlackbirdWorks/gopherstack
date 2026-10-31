package gcpbatch

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpbatch: nil app context")

// ConfigProvider extracts Batch configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPBatchSettings() Settings
}

// Provider implements service.Provider for Batch. It serves gRPC, REST and the gateway data plane on
// its own listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPBatch" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPBatchSettings()
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
	if settings.Engine != EngineDocker {
		return
	}

	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn(
			"gcpbatch: container runtime unavailable; jobs progress on the clock without running",
			"error",
			err,
		)

		return
	}

	backend.SetRuntime(rt, settings.ScriptImage)
}
