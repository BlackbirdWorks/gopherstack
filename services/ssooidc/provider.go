package ssooidc

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the IAM Identity Center OIDC API.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "SSOOIDC" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	return NewHandler(NewInMemoryBackend()), nil
}
