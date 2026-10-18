package pricing

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the AWS Price List API.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Pricing" }

// Init builds the handler; the catalog is embedded and stateless.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	return NewHandler(), nil
}
