package ram

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when the AppContext passed to Init is nil.
var ErrNilAppContext = errors.New("ram: nil AppContext")

// Provider implements service.Provider for AWS RAM.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "RAM" }

// Init initializes the RAM service backend and handler.
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
