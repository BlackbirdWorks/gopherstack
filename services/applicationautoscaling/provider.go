package applicationautoscaling

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Application Auto Scaling.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ApplicationAutoscaling" }

// Init initializes the Application Auto Scaling backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
