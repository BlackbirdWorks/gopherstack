package ocistreaming

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocistreaming: nil app context")

// ConfigProvider extracts OCI Streaming configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIStreamingSettings() Settings
}

// Provider implements service.Provider for OCI Streaming. It runs on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIStreaming" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIStreamingSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
