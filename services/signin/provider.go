package signin

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil application context.
var ErrNilAppContext = errors.New("appContext must not be nil")

// Provider implements service.Provider for AWS Sign-In.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return serviceName }

// Init creates the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
