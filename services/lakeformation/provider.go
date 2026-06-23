package lakeformation

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("lakeformation: nil AppContext")

// Provider implements service.Provider for the Lake Formation service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "LakeFormation" }

// Init initializes the Lake Formation service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend()
	backend.StartJanitor(ctx.JanitorCtx)

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
