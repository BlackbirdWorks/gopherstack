package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for AWS Network Manager.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "NetworkManager" }

// Init initializes the Network Manager service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(ctx.JanitorCtx, accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
