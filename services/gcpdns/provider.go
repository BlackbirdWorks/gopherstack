package gcpdns

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpdns: nil app context")

// ConfigProvider extracts Cloud DNS configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPDNSSettings() Settings
}

// Provider implements service.Provider for Google Cloud DNS. It serves the REST API and the DNS data plane
// on their own listeners (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPDNS" }

// Init creates the backend and handler; the ports are bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPDNSSettings()
	}

	handler := NewHandler(NewInMemoryBackend())
	handler.Port = settings.Port
	handler.DNSPort = settings.DNSPort

	return handler, nil
}
