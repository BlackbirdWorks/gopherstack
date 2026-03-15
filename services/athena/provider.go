package athena

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Athena service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Athena" }

// Init initializes the Athena backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.WithJanitor(0, 0)

	return handler, nil
}
