package timestreamwrite

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon Timestream Write.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "TimestreamWrite" }

// Init initializes the Timestream Write service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
