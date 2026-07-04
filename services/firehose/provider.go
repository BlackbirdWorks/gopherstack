package firehose

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to Firehose Provider.Init")

// Provider implements service.Provider for Kinesis Firehose.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Firehose" }

// Init initializes the Firehose service backend and handler.
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
