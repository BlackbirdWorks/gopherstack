package ocidns

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocidns: nil app context")

// ConfigProvider extracts OCI DNS configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIDNSSettings() Settings
}

// Provider implements service.Provider for OCI DNS. It runs on its own listeners (see Handler.StartWorker) and
// never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIDNS" }

// Init creates the backend and handler; the ports are bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIDNSSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port
	handler.DNSPort = settings.DNSPort
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
