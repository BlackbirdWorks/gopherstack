package kms

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the KMS service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "KMS"
}

// Init initializes the KMS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
