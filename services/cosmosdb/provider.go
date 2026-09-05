package cosmosdb

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("cosmosdb: nil app context")

// ConfigProvider is a private interface to extract CosmosDB configuration
// from the abstract AppContext Config, mirroring services/azuretable.ConfigProvider.
type ConfigProvider interface {
	GetCosmosDBSettings() Settings
}

// Provider implements service.Provider for the Azure Cosmos DB (Core/SQL
// API) service.
//
// Like services/azureblob, services/azurequeue, and services/azuretable,
// CosmosDB does not register a RouteMatcher into the shared AWS single-port
// Router: its /dbs/... resource hierarchy has no service-identifying header
// the way AWS's X-Amz-Target does, and there is no reason to multiplex it
// onto any of those three services' own dedicated ports either. Instead the
// returned Handler implements service.BackgroundWorker and stands up its own
// dedicated *echo.Echo/*http.Server, listening on a fixed,
// protocol-conventional port (the real Cosmos DB Local Emulator's own
// default, 8081 -- not any of Azurite's 10000/10001/10002, since Cosmos's
// real emulator is a different tool with its own different default; see
// AZURE.md section 4). It is registered in cli.go's
// getMostRecentServiceProviders like every other provider; only its
// RouteMatcher (which always returns false) is inert.
//
// Like services/azuretable, CosmosDB has no janitor: honoring a container's
// optional DefaultTimeToLive setting is out of scope for this milestone (see
// PARITY.md's deferred section), so there is nothing for a background sweep
// to do.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "CosmosDB" }

// Init initializes the CosmosDB service backend and handler. The configured
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
		settings = cp.GetCosmosDBSettings()
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.MasterKey = settings.MasterKey
	handler.ValidateAuth = settings.ValidateAuth

	return handler, nil
}
