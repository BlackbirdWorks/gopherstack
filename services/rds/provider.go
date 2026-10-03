package rds

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to RDS Provider.Init")

// EngineModeConfig exposes the RDS engine mode (stub or docker).
type EngineModeConfig interface {
	GetRDSEngine() string
}

// Provider implements service.Provider for RDS.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "RDS" }

// Init initializes the RDS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if ec, ok := ctx.Config.(EngineModeConfig); ok && ec.GetRDSEngine() == EngineDocker {
		enableDockerEngine(ctx, backend)
	}

	handler := NewHandler(backend)

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("RDS: container runtime unavailable; databases stay metadata-only", "error", err)

		return
	}

	er, ok := rt.(EngineRuntime)
	if !ok {
		ctx.Logger.Warn("RDS: container runtime cannot stop/start containers; databases stay metadata-only")

		return
	}

	silenceDriverLogs()
	backend.EnableEngine(EngineConfig{Runtime: er, Ports: ctx.PortAlloc, Host: os.Getenv("RDS_DB_HOST")})
}
