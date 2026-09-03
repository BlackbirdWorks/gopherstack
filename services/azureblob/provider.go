package azureblob

import (
	"errors"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azureblob: nil app context")

// Provider implements service.Provider for the Azure Blob Storage service.
//
// Unlike every other provider in this repo, AzureBlob does not register a
// RouteMatcher into the shared AWS single-port Router: Azure Blob's path
// shape (/<account>/<container>/<blob>) has no service-identifying header
// the way AWS's X-Amz-Target does, so multiplexing it onto the shared port
// risks exactly the collision the router avoids by construction for AWS
// services (see AZURE.md section 4). Instead the returned Handler implements
// service.BackgroundWorker and stands up its own dedicated *echo.Echo/
// *http.Server, listening on its own port -- mirroring Azurite's own
// separate-port-per-service convention (10000 for Blob).
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureBlob" }

// Init initializes the AzureBlob service backend and handler, resolving the
// dedicated port the handler's StartWorker will later listen on.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.Port = resolvePort(settings.Port, ctx.PortAlloc)

	return handler, nil
}

// resolvePort implements azureblob's port-selection strategy.
//
// gopherstack's pkgs/portalloc.Allocator only supports acquiring the next
// free port from a sequential range (Allocator.Acquire) -- it has no concept
// of reserving a *specific* preferred port. Every existing PortAlloc caller
// in this repo (Lambda function URLs, ElastiCache) wants an arbitrary
// ephemeral port and is fine with whatever it gets. Azure Blob is different:
// real SDKs default their connection strings/emulator constants to a *fixed*
// port (Azurite's 10000, see AZURE.md section 2), so gopherstack must
// actually try to bind that fixed port to be a useful drop-in target,
// falling back to the shared pool only when it can't.
//
// This is the "one real architectural decision" AZURE.md section 4 flags as
// a gap in the current single-port-router design: there is no existing
// precedent in the repo for a "give me this exact port or tell me it's
// busy" primitive, so this function bridges the gap locally instead of
// extending portalloc's contract (which would ripple to every other caller)
// for one service's needs.
//
// The availability probe is inherently racy (the port could be taken between
// this check and StartWorker's real bind) -- acceptable for a local dev/test
// emulator, not something a production load balancer would do.
func resolvePort(preferred int, alloc *portalloc.Allocator) int {
	if portAvailable(preferred) {
		return preferred
	}

	if alloc != nil {
		if p, err := alloc.Acquire("azureblob"); err == nil {
			return p
		}
	}

	return preferred
}

// portAvailable reports whether port can currently be bound on all
// interfaces.
func portAvailable(port int) bool {
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return false
	}

	_ = l.Close()

	return true
}
