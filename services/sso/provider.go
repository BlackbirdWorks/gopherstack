package sso

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the IAM Identity Center portal API.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "SSOPortal" }

// Init builds the handler; the cli wires the token source and directory lazily.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	return NewHandler(), nil
}
