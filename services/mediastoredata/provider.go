package mediastoredata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for MediaStore Data.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MediaStoreData" }

// Init initializes the MediaStore Data backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	region := config.DefaultRegion

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			region = cp.GetGlobalConfig().GetRegion()
		}
	}

	backend := NewInMemoryBackend(region)
	handler := NewHandler(backend)

	return handler, nil
}
