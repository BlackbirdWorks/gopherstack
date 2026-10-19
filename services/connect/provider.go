// Package connect emulates the Amazon Connect control plane and the contact records behind its metrics.
package connect

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("connect: AppContext must not be nil")

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Connect" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(NewBackend(accountID, region))
	h.DefaultRegion = region

	return h, nil
}
