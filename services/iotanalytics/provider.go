package iotanalytics

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// Provider implements service.Provider for the IoT Analytics service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "IoTAnalytics" }

// Init initializes the IoT Analytics service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(appCtx *service.AppContext) (service.Registerable, error) {
	if appCtx == nil {
		return nil, ErrNilAppContext
	}

	backend := NewInMemoryBackendWithContext(appCtx.JanitorCtx)
	handler := NewHandler(backend)

	return handler, nil
}
