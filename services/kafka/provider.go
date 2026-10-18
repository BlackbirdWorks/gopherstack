package kafka

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Kafka Provider.Init")

// Engine modes for the MSK service.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// EngineConfig exposes the MSK engine mode (stub or docker).
type EngineConfig interface {
	GetKafkaEngine() string
}

// EngineCADirConfig locates the directory that persists the broker CA; empty keeps it in memory.
type EngineCADirConfig interface {
	GetKafkaCADir() string
}

// Provider implements service.Provider for MSK (Kafka).
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Kafka" }

// Init initializes the Kafka backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if ec, ok := ctx.Config.(EngineConfig); ok && ec.GetKafkaEngine() == EngineDocker {
		rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
		if err != nil {
			ctx.Logger.Warn("Kafka: container runtime unavailable; clusters stay metadata-only", "error", err)
		} else {
			backend.EnableBrokers(BrokerConfig{
				Runtime: rt,
				Ports:   ctx.PortAlloc,
				Host:    os.Getenv("KAFKA_BROKER_HOST"),
				CA:      loadBrokerCA(ctx),
			})
		}
	}

	handler := NewHandler(backend)

	return handler, nil
}

func loadBrokerCA(ctx *service.AppContext) *devtls.CA {
	cc, ok := ctx.Config.(EngineCADirConfig)
	if !ok || cc.GetKafkaCADir() == "" {
		return nil
	}

	ca, err := devtls.LoadOrCreateCA(cc.GetKafkaCADir())
	if err != nil {
		ctx.Logger.Warn("Kafka: persisted broker CA unavailable; using an in-memory CA", "error", err)

		return nil
	}

	return ca
}
