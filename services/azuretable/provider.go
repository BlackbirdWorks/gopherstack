package azuretable

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azuretable: nil app context")

// ConfigProvider is a private interface to extract AzureTable configuration
// from the abstract AppContext Config, mirroring services/azurequeue.ConfigProvider.
type ConfigProvider interface {
	GetAzureTableSettings() Settings
}

// Provider implements service.Provider for the Azure Table Storage service.
//
// Like services/azureblob and services/azurequeue, AzureTable does not
// register a RouteMatcher into the shared AWS single-port Router: Azure
// Table's path shape (/<account>/<resource>) has no service-identifying
// header the way AWS's X-Amz-Target does, and shares the same
// /<account>/<resource> shape as Azure Blob and Queue, so multiplexing it
// onto the shared port (or either of their own dedicated ports) risks
// exactly the collision the router avoids by construction for AWS services
// (see AZURE.md section 4). Instead the returned Handler implements
// service.BackgroundWorker and stands up its own dedicated
// *echo.Echo/*http.Server, listening on a fixed, protocol-conventional port
// (Azurite's own Table port, 10002). It is registered in cli.go's
// getMostRecentServiceProviders like every other provider; only its
// RouteMatcher (which always returns false) is inert.
//
// Unlike services/azureblob and services/azurequeue, AzureTable has no
// janitor: Table Storage entities carry no TTL/expiry concept for a
// background sweep to enforce.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureTable" }

// Init initializes the AzureTable service backend and handler. The
// configured port (Settings.Port, default DefaultPort) is only recorded
// here; the actual TCP bind happens synchronously in Handler.StartWorker, so
// a port-in-use failure is returned to the caller directly instead of being
// discovered later from a background goroutine.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureTableSettings()
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
