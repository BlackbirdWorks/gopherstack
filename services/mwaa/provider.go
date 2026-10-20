package mwaa

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to MWAA Provider.Init")

// Provider implements service.Provider for the MWAA service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MWAA" }

// Init initializes the MWAA service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(region, accountID)
	backend.SetAppConfig(ctx.Config)

	if ec, ok := ctx.Config.(EngineConfig); ok && ec.GetMWAAEngine() == EngineDocker {
		enableDockerAirflow(ctx, backend, ec.GetMWAAEndpointURL())
	}

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}

func enableDockerAirflow(ctx *service.AppContext, backend *InMemoryBackend, endpoint string) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("MWAA: container runtime unavailable; environments stay state-only", "error", err)

		return
	}

	backend.EnableAirflow(AirflowConfig{Runtime: rt, Ports: ctx.PortAlloc, EndpointURL: endpoint})
}
