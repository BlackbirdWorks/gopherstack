package timestreamwrite

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("timestreamwrite: nil app context")

// Provider implements service.Provider for Amazon Timestream Write.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "TimestreamWrite" }

// Init initializes the Timestream Write service backend and handler.
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
