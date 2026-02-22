package sqs

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the SQS service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "SQS" }

// Init initializes the SQS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
