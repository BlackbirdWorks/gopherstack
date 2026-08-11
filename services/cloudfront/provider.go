package cloudfront

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for AWS CloudFront.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CloudFront" }

// Init initializes the CloudFront service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	janitorCtx := context.Background()

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.GetAccountID()
			region = cfg.GetRegion()
		}

		if ctx.JanitorCtx != nil {
			janitorCtx = ctx.JanitorCtx
		}
	}

	backend := NewInMemoryBackend(janitorCtx, accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
