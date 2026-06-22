package codecommit

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// Provider implements service.Provider for AWS CodeCommit.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CodeCommit" }

// Init initializes the CodeCommit service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
