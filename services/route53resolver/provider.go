package route53resolver

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Route53Resolver Provider.Init")

// Provider implements service.Provider for Route 53 Resolver.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Route53Resolver" }

// Init initializes the Route 53 Resolver service backend and handler.
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
