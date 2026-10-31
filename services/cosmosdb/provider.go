package cosmosdb

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("cosmosdb: nil app context")

// ConfigProvider is a private interface to extract CosmosDB configuration
// from the abstract AppContext Config, mirroring services/azuretable.ConfigProvider.
type ConfigProvider interface {
	GetCosmosDBSettings() Settings
}

// Provider implements service.Provider for the Azure Cosmos DB service. It registers no RouteMatcher on the
// shared single-port Router: the returned Handler is a service.BackgroundWorker serving its own listener on the
// fixed Cosmos emulator port (8081), and also runs the TTL sweeper.
type Provider struct{}

const (
	// envTLS opts into an HTTPS listener with an in-memory self-signed certificate.
	envTLS = "COSMOSDB_TLS"
	// envTLSHosts lists extra comma-separated DNS names or IPs for that certificate.
	envTLSHosts = "COSMOSDB_TLS_HOSTS"
)

func splitHosts(raw string) []string {
	var hosts []string

	for h := range strings.SplitSeq(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}

	return hosts
}

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

	m := settings.ResourceTokenMaxSeconds
	if m != 0 && (m < MinResourceTokenMaxSeconds || m > MaxResourceTokenMaxSeconds) {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidResourceTokenMax, m)
	}

	backend := NewInMemoryBackend()
	backend.SetServerless(settings.Serverless)
	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.MasterKey = settings.MasterKey
	handler.ValidateAuth = settings.ValidateAuth
	handler.ResourceTokenMaxSeconds = settings.ResourceTokenMaxSeconds
	handler.EnforceThroughput = settings.EnforceThroughput
	handler.BurstCapacity = settings.BurstCapacity
	handler.TLS = os.Getenv(envTLS) == trueText
	handler.TLSHosts = splitHosts(os.Getenv(envTLSHosts))

	return handler, nil
}
