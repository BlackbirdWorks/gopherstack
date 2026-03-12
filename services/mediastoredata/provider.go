package mediastoredata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for MediaStore Data.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MediaStoreData" }

// Init initializes the MediaStore Data backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
