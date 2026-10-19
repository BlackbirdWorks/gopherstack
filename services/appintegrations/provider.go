// Package appintegrations emulates Amazon AppIntegrations: event and data integrations, their associations,
// and applications.
package appintegrations

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("appintegrations: AppContext must not be nil")

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "AppIntegrations" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(NewInMemoryBackend(accountID, region))
	h.DefaultRegion = region

	return h, nil
}
