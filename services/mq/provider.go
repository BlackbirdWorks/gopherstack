package mq

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when the AppContext passed to Init is nil.
var ErrNilAppContext = errors.New("mq: nil AppContext")

// Provider implements service.Provider for Amazon MQ.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MQ" }

// Init initializes the Amazon MQ backend and handler.
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
