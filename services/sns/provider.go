package sns

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("sns: nil app context")

// Provider implements service.Provider for the SNS service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "SNS"
}

// Init initializes the SNS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend
	var defaultRegion string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, cfg.GetAccountID(), cfg.GetRegion())
		defaultRegion = cfg.GetRegion()
	} else {
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, "", "")
	}

	handler := NewHandler(backend)
	handler.DefaultRegion = defaultRegion

	return handler, nil
}
