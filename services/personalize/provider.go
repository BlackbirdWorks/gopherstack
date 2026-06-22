package personalize

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("personalize: nil AppContext")

// Provider implements service.Provider for the Personalize service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Personalize" }

// Init initializes the Personalize backend and handler.
//
//nolint:ireturn,nolintlint // Provider contract returns service interface.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
