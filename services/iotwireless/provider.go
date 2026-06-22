package iotwireless

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is provided.
var ErrNilAppContext = errors.New("iotwireless: nil AppContext")

// Provider implements service.Provider for the IoT Wireless service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "IoTWireless" }

// Init initializes the IoT Wireless service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
