package mq

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when the AppContext passed to Init is nil.
var ErrNilAppContext = errors.New("mq: nil AppContext")

// Engine modes for Amazon MQ.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// EngineConfig exposes the Amazon MQ engine mode (stub or docker).
type EngineConfig interface {
	GetMQEngine() string
}

// Provider implements service.Provider for Amazon MQ.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MQ" }

// Init initializes the Amazon MQ backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if ec, ok := ctx.Config.(EngineConfig); ok && ec.GetMQEngine() == EngineDocker {
		rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
		if err != nil {
			ctx.Logger.Warn("MQ: container runtime unavailable; brokers stay metadata-only", "error", err)
		} else {
			backend.EnableBrokers(BrokerConfig{Runtime: rt, Ports: ctx.PortAlloc, Host: os.Getenv("MQ_BROKER_HOST")})
		}
	}

	handler := NewHandler(backend)
	handler.EnableRegions()

	return handler, nil
}
