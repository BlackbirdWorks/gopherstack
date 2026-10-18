// Package amp emulates Amazon Managed Service for Prometheus: the control plane and the Prometheus
// remote-write and HTTP query data plane.
package amp

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for Amazon Managed Service for Prometheus.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "AMP" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
