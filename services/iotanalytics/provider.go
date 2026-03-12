package iotanalytics

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the IoT Analytics service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "IoTAnalytics" }

// Init initializes the IoT Analytics service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
