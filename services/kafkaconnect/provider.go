package kafkaconnect

import (
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// EngineDocker is the engine mode that runs real Kafka Connect workers; it shares MSK's --kafka-engine switch.
const EngineDocker = "docker"

// EngineConfig exposes the shared MSK engine mode (stub or docker).
type EngineConfig interface {
	GetKafkaEngine() string
}

func enableDockerWorkers(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("KafkaConnect: container runtime unavailable; connectors stay metadata-only", "error", err)

		return
	}

	backend.EnableWorkers(WorkerConfig{Runtime: rt, Ports: ctx.PortAlloc, Host: os.Getenv("KAFKA_CONNECT_HOST")})
}

// Provider implements service.Provider for Amazon MSK Connect.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "KafkaConnect" }

// Init initializes the MSK Connect service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend()
	backend.SetAppConfig(ctx.Config)

	if ec, ok := ctx.Config.(EngineConfig); ok && ec.GetKafkaEngine() == EngineDocker {
		enableDockerWorkers(ctx, backend)
	}

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	handler.EnableRegions()

	return handler, nil
}
