package textract

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init receives a nil AppContext.
var ErrNilAppContext = errors.New("textract: nil AppContext")

// Provider implements service.Provider for Amazon Textract.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Textract" }

// Init initializes the Textract service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackendWithContext(ctx.JanitorCtx, accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
