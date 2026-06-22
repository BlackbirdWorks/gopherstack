package pinpoint

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init receives a nil AppContext.
var ErrNilAppContext = errors.New("pinpoint: nil AppContext")

// Provider implements service.Provider for the Pinpoint service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Pinpoint" }

// Init initializes the Pinpoint service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(region, accountID)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
