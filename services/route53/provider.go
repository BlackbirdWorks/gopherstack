package route53

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("route53: AppContext is nil")

// Provider implements service.Provider for the Route 53 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Route53" }

// Init initializes the Route 53 service backend and handler.
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
