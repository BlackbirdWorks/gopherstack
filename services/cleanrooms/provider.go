package cleanrooms

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to CleanRooms Provider.Init")

// Provider implements service.Provider for AWS Clean Rooms.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CleanRooms" }

// Init initializes the Clean Rooms service backend and handler.
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

	backend := NewInMemoryBackendWithContext(ctx.JanitorCtx, accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
