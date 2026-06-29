package bedrockruntime

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Bedrock Runtime.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BedrockRuntime" }

// Init initializes the Bedrock Runtime backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.GetAccountID()
			region = cfg.GetRegion()
		}
	}

	var svcCtx context.Context
	if ctx != nil {
		svcCtx = ctx.JanitorCtx
	}

	backend := NewInMemoryBackendWithContext(svcCtx, accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
