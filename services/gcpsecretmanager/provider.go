package gcpsecretmanager

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpsecretmanager: nil app context")

// ConfigProvider extracts Secret Manager configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPSecretManagerSettings() Settings
}

// Provider implements service.Provider for Google Secret Manager. It serves
// gRPC on its own listener (see Handler.StartWorker) and never matches on the
// shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPSecretManager" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPSecretManagerSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port

	return handler, nil
}
