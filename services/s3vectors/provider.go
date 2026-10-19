package s3vectors

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for Amazon S3 Vectors.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "S3Vectors" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(NewInMemoryBackend(accountID, region))
	h.DefaultRegion = region

	return h, nil
}
