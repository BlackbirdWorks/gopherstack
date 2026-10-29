package ocinosql

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocinosql: nil app context")

// ConfigProvider extracts OCI NoSQL configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCINoSQLSettings() Settings
}

// Provider implements service.Provider for OCI NoSQL. It runs on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCINoSQL" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCINoSQLSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
