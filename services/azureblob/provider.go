package azureblob

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azureblob: nil app context")

// ConfigProvider is a private interface to extract AzureBlob configuration
// from the abstract AppContext Config, mirroring services/s3.ConfigProvider.
type ConfigProvider interface {
	GetAzureBlobSettings() Settings
}

// Provider implements service.Provider for the Azure Blob Storage service.
//
// Unlike every other provider in this repo, AzureBlob does not register a
// RouteMatcher into the shared AWS single-port Router: Azure Blob's path
// shape (/<account>/<container>/<blob>) has no service-identifying header
// the way AWS's X-Amz-Target does, so multiplexing it onto the shared port
// risks exactly the collision the router avoids by construction for AWS
// services (see AZURE.md section 4). Instead the returned Handler implements
// service.BackgroundWorker and stands up its own dedicated *echo.Echo/
// *http.Server, listening on a fixed, protocol-conventional port -- the same
// pattern services/iot's MQTT broker already uses in this repo for a
// well-known port (1883) that isn't part of the shared AWS request/response
// cycle. It is registered in cli.go's getMostRecentServiceProviders like
// every other provider; only its RouteMatcher (which always returns false)
// is inert.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureBlob" }

// Init initializes the AzureBlob service backend and handler. The configured
// port (Settings.Port, default DefaultPort) is only recorded here; the
// actual TCP bind happens synchronously in Handler.StartWorker, so a
// port-in-use failure is returned to the caller directly instead of being
// discovered later from a background goroutine.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureBlobSettings()
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
