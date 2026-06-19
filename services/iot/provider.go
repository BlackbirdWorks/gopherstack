package iot

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// Provider implements service.Provider for the IoT service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "IoT" }

// Init initialises the IoT backend, MQTT broker, and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.GetAccountID(), cfg.GetRegion())
	} else {
		backend = NewInMemoryBackend()
	}

	broker := NewBroker(backend, backend.MQTTPort())
	handler := NewHandler(backend, broker)

	return handler, nil
}
