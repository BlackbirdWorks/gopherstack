package redshift

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Redshift Provider.Init")

// ErrNilAppContextSL is returned by ServerlessProvider.Init when a nil AppContext is passed.
var ErrNilAppContextSL = errors.New("nil AppContext passed to Redshift ServerlessProvider.Init")

// Provider implements service.Provider for Redshift.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Redshift" }

// Init initializes the Redshift service backend and handler.
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

// ServerlessProvider implements service.Provider for Redshift Serverless.
type ServerlessProvider struct{}

// Name returns the provider name.
func (p *ServerlessProvider) Name() string { return "RedshiftServerless" }

// Init initializes the Redshift Serverless backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *ServerlessProvider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContextSL
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewServerlessHandler(backend)

	return handler, nil
}
