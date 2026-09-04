package azurequeue

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azurequeue: nil app context")

// ConfigProvider is a private interface to extract AzureQueue configuration
// from the abstract AppContext Config, mirroring services/azureblob.ConfigProvider.
type ConfigProvider interface {
	GetAzureQueueSettings() Settings
}

// Provider implements service.Provider for the Azure Queue Storage service.
//
// Like services/azureblob, AzureQueue does not register a RouteMatcher into
// the shared AWS single-port Router: Azure Queue's path shape
// (/<account>/<queue>[/messages[/<id>]]) has no service-identifying header
// the way AWS's X-Amz-Target does, and shares the same /<account>/<resource>
// shape as Azure Blob and Table, so multiplexing it onto the shared port (or
// even onto Azure Blob's own dedicated port) risks exactly the collision the
// router avoids by construction for AWS services (see AZURE.md section 4).
// Instead the returned Handler implements service.BackgroundWorker and
// stands up its own dedicated *echo.Echo/*http.Server, listening on a fixed,
// protocol-conventional port (Azurite's own Queue port, 10001). It is
// registered in cli.go's getMostRecentServiceProviders like every other
// provider; only its RouteMatcher (which always returns false) is inert.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureQueue" }

// Init initializes the AzureQueue service backend and handler. The
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
		settings = cp.GetAzureQueueSettings()
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend).WithJanitor(0)
	handler.Port = settings.Port

	return handler, nil
}
