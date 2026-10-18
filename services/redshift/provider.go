package redshift

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Redshift Provider.Init")

// ErrNilAppContextSL is returned by ServerlessProvider.Init when a nil AppContext is passed.
var ErrNilAppContextSL = errors.New("nil AppContext passed to Redshift ServerlessProvider.Init")

// Provider implements service.Provider for Redshift.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Redshift" }

// Init initializes the Redshift service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	if ec, ok := ctx.Config.(EngineModeConfig); ok && ec.GetRedshiftEngine() == EngineDocker {
		enableDockerEngine(ctx, backend)
	}

	handler := NewHandler(backend)
	handler.EnableRegions()

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("Redshift: container runtime unavailable; clusters stay metadata-only", "error", err)

		return
	}

	er, ok := rt.(EngineRuntime)
	if !ok {
		ctx.Logger.Warn("Redshift: container runtime cannot stop/start containers; clusters stay metadata-only")

		return
	}

	backend.EnableEngine(EngineConfig{Runtime: er, Ports: ctx.PortAlloc, Host: os.Getenv("REDSHIFT_HOST")})
}

// ServerlessProvider implements service.Provider for Redshift Serverless.
type ServerlessProvider struct{}

// Name returns the provider name.
func (p *ServerlessProvider) Name() string { return "RedshiftServerless" }

// Init initializes the Redshift Serverless backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *ServerlessProvider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContextSL
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewServerlessHandler(backend)
	handler.EnableRegions()

	return handler, nil
}
