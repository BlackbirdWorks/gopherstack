package servicediscovery

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when a nil AppContext is passed to Provider.Init.
var ErrNilAppContext = errors.New("nil AppContext passed to ServiceDiscovery Provider.Init")

// Provider implements service.Provider for AWS Cloud Map (Service Discovery).
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ServiceDiscovery" }

// Init initializes the Service Discovery backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
