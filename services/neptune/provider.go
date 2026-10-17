package neptune

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Neptune Provider.Init")

// Provider implements service.Provider for Neptune.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Neptune" }

// Init initializes the Neptune service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if ec, ok := ctx.Config.(EngineModeConfig); ok && ec.GetNeptuneEngine() == EngineDocker {
		enableDockerEngine(ctx, backend)
	}

	handler := NewHandler(backend)

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("Neptune: container runtime unavailable; clusters stay metadata-only", "error", err)

		return
	}

	er, ok := rt.(EngineRuntime)
	if !ok {
		ctx.Logger.Warn("Neptune: container runtime cannot stop/start containers; clusters stay metadata-only")

		return
	}

	backend.EnableEngine(EngineConfig{Runtime: er, Ports: ctx.PortAlloc, Host: os.Getenv("NEPTUNE_HOST")})
}
