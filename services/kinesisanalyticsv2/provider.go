// Package kinesisanalyticsv2 emulates AWS Managed Service for Apache Flink (Kinesis Data Analytics v2).
package kinesisanalyticsv2

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Provider.Init when a nil AppContext is supplied.
var ErrNilAppContext = errors.New("kinesisanalyticsv2: AppContext must not be nil")

// EngineDocker is the engine mode that runs real Flink jobs for StartApplication.
const EngineDocker = "docker"

// EngineConfig exposes the Flink engine mode (stub or docker) and the endpoint containers use to reach gopherstack.
type EngineConfig interface {
	GetFlinkEngine() string
	GetFlinkEndpointURL() string
}

// Provider implements service.Provider for Kinesis Data Analytics v2.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "KinesisAnalyticsV2" }

// Init initializes the Kinesis Data Analytics v2 backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	backend.SetAppConfig(ctx.Config)

	if ec, ok := ctx.Config.(EngineConfig); ok && ec.GetFlinkEngine() == EngineDocker {
		enableDockerFlink(ctx, backend, ec.GetFlinkEndpointURL())
	}

	return NewHandler(backend), nil
}

func enableDockerFlink(ctx *service.AppContext, backend *InMemoryBackend, endpoint string) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("KinesisAnalyticsV2: container runtime unavailable; applications stay state-only", "error", err)

		return
	}

	backend.EnableFlink(FlinkConfig{Runtime: rt, Ports: ctx.PortAlloc, EndpointURL: endpoint})
}
