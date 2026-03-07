package iot

import (
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the IoT service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "IoT" }

// Init initialises the IoT backend, MQTT broker, and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.AccountID, cfg.Region)
	} else {
		backend = NewInMemoryBackend()
	}

	broker := NewBroker(backend, backend.MQTTPort(), slog.Default())
	handler := NewHandler(backend, broker)

	return handler, nil
}
