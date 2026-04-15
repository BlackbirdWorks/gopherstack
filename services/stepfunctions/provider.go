package stepfunctions

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("stepfunctions: nil app context")

// Provider implements service.Provider for the Step Functions service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "StepFunctions" }

// Init initializes the Step Functions service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, cfg.GetAccountID(), cfg.GetRegion())
	} else {
		backend = NewInMemoryBackend()
	}

	handler := NewHandler(backend)

	return handler, nil
}
