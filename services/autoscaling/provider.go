package autoscaling

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Autoscaling service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Autoscaling" }

// Init initializes the Autoscaling backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return NewHandler(NewInMemoryBackend()), nil
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackendWithConfig(accountID, region)
	backend.ec2Lookup = ec2LookupAdapter{cfg: ctx.Config}

	handler := NewHandler(backend)
	handler.EnableRegions(ctx.JanitorCtx)

	return handler, nil
}
