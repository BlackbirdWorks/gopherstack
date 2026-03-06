package s3control

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for S3 Control.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "S3Control" }

// Init initializes the S3 Control service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
