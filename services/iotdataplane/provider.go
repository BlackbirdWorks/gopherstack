package iotdataplane

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// Provider implements service.Provider for the IoT Data Plane service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "IoTDataPlane" }

// Init initialises the IoT Data Plane backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
