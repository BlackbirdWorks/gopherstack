package mediastore

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the MediaStore service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MediaStore" }

// Init initializes the MediaStore service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
