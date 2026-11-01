package elbv2

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the ELBv2 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "ELBv2" }

// ListenerModeConfig exposes the --elbv2-listeners setting.
type ListenerModeConfig interface {
	GetELBv2Listeners() string
	GetELBv2ListenerHost() string
}

// Init initializes the ELBv2 backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	if lc, ok := ctx.Config.(ListenerModeConfig); ok && lc.GetELBv2Listeners() == ListenerModeReal {
		backend.EnableRealListeners(RealListenerConfig{Ports: ctx.PortAlloc, Host: lc.GetELBv2ListenerHost()})
	}

	handler := NewHandler(backend)
	handler.EnableRegions()

	return handler, nil
}
