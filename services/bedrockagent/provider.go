// Package bedrockagent provides a local stub for the Amazon Bedrock Agent service.
package bedrockagent

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when a nil AppContext is passed to Provider.Init.
var ErrNilAppContext = errors.New("bedrockagent: AppContext must not be nil")

// Provider implements service.Provider for the Bedrock Agent service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BedrockAgent" }

// Init initialises the Bedrock Agent backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.GetAccountID()
		region = cfg.GetRegion()
	}

	backend := NewInMemoryBackend(region, accountID)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
