package verifiedpermissions

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when a nil AppContext is passed to Init.
var ErrNilAppContext = errors.New("verifiedpermissions: nil AppContext")

// Provider implements service.Provider for Amazon Verified Permissions.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "VerifiedPermissions" }

// Init initializes the Verified Permissions service backend and handler.
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
