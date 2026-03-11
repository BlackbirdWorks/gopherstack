package elasticbeanstalk

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Elastic Beanstalk service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Elasticbeanstalk" }

// Init initializes the Elastic Beanstalk backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	handler := NewHandler(backend)

	return handler, nil
}
