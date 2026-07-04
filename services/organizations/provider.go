package organizations

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init receives a nil AppContext.
var ErrNilAppContext = errors.New("organizations: AppContext is nil")

// Provider implements service.Provider for the Organizations service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Organizations" }

// Init initializes the Organizations service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
