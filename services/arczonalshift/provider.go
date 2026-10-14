// Package arczonalshift emulates Application Recovery Controller zonal shift and zonal autoshift.
package arczonalshift

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("arczonalshift: AppContext must not be nil")

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ARCZonalShift" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID)
	backend.SetAppConfig(ctx.Config)

	handler := NewHandler(backend)
	handler.DefaultRegion = region

	return handler, nil
}
