package sqs

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("sqs: nil app context")

// Provider implements service.Provider for the SQS service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "SQS" }

// Init initializes the SQS service backend and handler.
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
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, config.DefaultAccountID, config.DefaultRegion)
	}

	handler := NewHandler(backend).WithJanitor()
	handler.DefaultRegion = defaultRegion

	return handler, nil
}
