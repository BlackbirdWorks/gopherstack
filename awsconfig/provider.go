package awsconfig

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for AWS Config.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "AWSConfig" }

// Init initializes the AWS Config service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
