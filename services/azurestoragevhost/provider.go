package azurestoragevhost

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract this service's
// configuration from the abstract AppContext Config, mirroring
// services/azureblob.ConfigProvider.
type ConfigProvider interface {
	GetAzureStorageVHostSettings() Settings
}

// Provider implements service.Provider for the shared Azure Storage
// virtual-hosted-style listener. See handler.go's package doc comment for
// why this exists.
//
// Like services/azureblob/azurequeue/azuretable, this does not register a
// RouteMatcher into the shared AWS single-port Router -- it runs on its own
// dedicated listener. It is registered in cli.go's getServiceProviders like
// every other provider; Blob/Queue/Table are wired into the returned
// Handler afterward, by cli.go's cross-service wiring (they aren't
// guaranteed to exist yet during Init()).
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureStorageVHost" }

// Init initializes the virtual-hosted storage Handler. The configured port
// (Settings.Port, default DefaultPort) is only recorded here; the actual
// TCP bind happens synchronously in Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	settings := DefaultSettings()
	if ctx != nil {
		if cp, ok := ctx.Config.(ConfigProvider); ok {
			settings = cp.GetAzureStorageVHostSettings()
		}
	}

	handler := NewHandler()
	handler.Port = settings.Port

	return handler, nil
}
