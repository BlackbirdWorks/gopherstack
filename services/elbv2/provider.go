package elbv2

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the ELBv2 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "ELBv2" }

// Init initializes the ELBv2 backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
