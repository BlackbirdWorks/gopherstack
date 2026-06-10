package dax

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("dax: nil app context")

// Provider implements service.Provider for Amazon DAX.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "DAX" }

// Init initializes the DAX service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.GetAccountID()
		region = cfg.GetRegion()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.EnableDataPlane(ctx.Logger, defaultDataPlaneAddr)

	return handler, nil
}
