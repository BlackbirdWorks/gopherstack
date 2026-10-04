package transcribe

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("transcribe: nil app context")

// Provider implements service.Provider for Amazon Transcribe.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Transcribe" }

// Init initializes the Transcribe service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	_, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackendForRegion(region)
	handler := NewHandler(backend)

	handler.EnableRegions()

	return handler, nil
}
