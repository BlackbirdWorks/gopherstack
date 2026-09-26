package kinesisvideo

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the Kinesis Video Streams service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "KinesisVideo" }

// Init initializes the Kinesis Video Streams service backend and handler.
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
