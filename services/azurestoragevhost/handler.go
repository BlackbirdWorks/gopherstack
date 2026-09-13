// Package azurestoragevhost is a thin translation layer, not a storage
// service in its own right: it owns one dedicated listener that accepts
// virtual-hosted-style Azure Storage requests (Host: "{account}.blob.host:port",
// ".queue.", ".table.") and rewrites them into the path-style requests
// services/azureblob, services/azurequeue, and services/azuretable already
// serve (Host: anything, path "/{account}/...").
//
// Why this exists: terraform-provider-azurerm's Blob/Queue/Table data-plane
// SDK (jackofallops/giovanni) hard-requires virtual-hosted-style URLs for
// every one of azurerm_storage_container/_blob/_queue/_table's post-create
// Read calls (AZURE.md section 10.8's M8 finding). gopherstack's M0-M2
// Blob/Queue/Table services are path-style (Azurite-style,
// "{host}:{port}/{account}/...") -- an already-shipped, independently
// correct design this package does not change. Both styles now work
// simultaneously against the exact same backend state: creating a container
// through this listener and listing it through services/azureblob's own
// listener (or vice versa) see the same data, because this package never
// duplicates business logic -- it only rewrites the request, then calls the
// real service's own Handler() unchanged.
//
// A structural constraint (not a design choice) is why Blob/Queue/Table
// must share one port here even though they keep three separate ports for
// path-style access: terraform-provider-azurerm's StorageDomainSuffix is a
// single shared string used to parse every one of these resources' account
// IDs back out of their endpoint URL, and Go's url.URL.Host always includes
// the port -- so one shared suffix can only match a host:port that is
// itself shared across all three services. See AZURE.md section 10.8 for
// the full derivation.
package azurestoragevhost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// Service-type labels this listener recognizes as the second Host-header
// segment, e.g. "myaccount.blob.localhost:10010".
const (
	blobServiceLabel  = "blob"
	queueServiceLabel = "queue"
	tableServiceLabel = "table"
)

const (
	vhostReadHeaderTimeout = 10 * time.Second
	vhostReadTimeout       = 60 * time.Second
	vhostIdleTimeout       = 120 * time.Second
)

// StorageHandler is the subset of *azureblob.Handler / *azurequeue.Handler /
// *azuretable.Handler this package needs: just the Echo handler each
// already exposes. Kept as an interface (rather than importing those three
// concrete packages) so this package has no compile-time dependency on
// their internals -- wiring the concrete *Handler values in happens once,
// in cli.go's cross-service wiring, exactly like every other
// already-initialized-handler dependency in this repo (e.g. wireSNSToSQS).
type StorageHandler interface {
	Handler() echo.HandlerFunc
}

// Handler is the Echo HTTP handler for the shared virtual-hosted-style
// Azure Storage listener. It holds no storage state of its own -- Blob,
// Queue, and Table each remain owned entirely by their own service; see the
// package doc comment.
type Handler struct {
	// Blob, Queue, and Table are the real services' handlers, wired in
	// post-construction by cli.go's cross-service wiring (Init() runs
	// before those services' own Init() calls are guaranteed to have
	// happened, so these start nil and are set once all services exist).
	Blob  StorageHandler
	Queue StorageHandler
	Table StorageHandler

	srvMu *lockmetrics.RWMutex
	srv   *http.Server

	// Port is the TCP port StartWorker binds. Set from Settings at Init
	// time; defaults to DefaultPort. Like services/azureblob, this is a
	// single fixed port with no fallback pool -- StartWorker fails fast if
	// it's unavailable.
	Port int
}

// NewHandler creates a new virtual-hosted storage Handler. Port defaults to
// DefaultPort; callers (typically provider.go) override it from Settings.
// Blob/Queue/Table are nil until wired by cli.go's cross-service wiring.
func NewHandler() *Handler {
	return &Handler{
		Port:  DefaultPort,
		srvMu: lockmetrics.New("azurestoragevhost.server"),
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "AzureStorageVHost" }

// GetSupportedOperations returns no operations of its own: every request
// this listener accepts is delegated verbatim to Blob/Queue/Table's own
// Handler(), which is what actually reports the operation for metrics (see
// ExtractOperation).
func (h *Handler) GetSupportedOperations() []string { return nil }

// RouteMatcher exists only to satisfy service.Registerable's interface
// contract: like services/azureblob/azurequeue/azuretable, this runs on its
// own dedicated listener started by StartWorker, never on the shared AWS
// single-port Router.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority returns the routing priority for this handler. Irrelevant
// in practice since RouteMatcher never matches; 0 (lowest) is the safe
// default.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation delegates to the resolved service's own ExtractOperation
// where possible; metrics for requests that don't resolve to a known
// service/account are labeled "Unknown".
func (h *Handler) ExtractOperation(c *echo.Context) string {
	_, svc, ok := accountAndServiceFromHost(c.Request().Host)
	if !ok {
		return "Unknown"
	}

	return "VHost" + strings.ToUpper(svc[:1]) + svc[1:]
}

// ExtractResource returns the account name resolved from the Host header,
// for metrics labeling.
func (h *Handler) ExtractResource(c *echo.Context) string {
	account, _, ok := accountAndServiceFromHost(c.Request().Host)
	if !ok {
		return ""
	}

	return account
}

// Handler returns the Echo handler function for the virtual-hosted storage
// listener: parse the account/service out of the Host header, rewrite the
// request path to the path-style shape the real service expects, and
// delegate to it unchanged.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		account, svc, ok := accountAndServiceFromHost(r.Host)
		if !ok {
			return c.String(http.StatusBadRequest,
				"azurestoragevhost: Host header must be \"<account>.{blob,queue,table}.<host:port>\"")
		}

		target, err := h.handlerFor(svc)
		if err != nil {
			return c.String(http.StatusBadRequest, "azurestoragevhost: "+err.Error())
		}

		originalPath := r.URL.Path
		r.URL.Path = "/" + account + originalPath

		defer func() { r.URL.Path = originalPath }()

		return target.Handler()(c)
	}
}

// errUnwiredService is returned when a virtual-hosted request resolves to a
// service (blob/queue/table) whose handler hasn't been wired yet -- should
// only happen if cli.go's cross-service wiring didn't run, since every
// service registered alongside this one always is.
var errUnwiredService = errors.New("service not wired")

func (h *Handler) handlerFor(svc string) (StorageHandler, error) {
	var target StorageHandler

	switch svc {
	case blobServiceLabel:
		target = h.Blob
	case queueServiceLabel:
		target = h.Queue
	case tableServiceLabel:
		target = h.Table
	}

	if target == nil {
		return nil, fmt.Errorf("%s: %w", svc, errUnwiredService)
	}

	return target, nil
}

// accountAndServiceFromHost parses a virtual-hosted-style Host header
// ("myaccount.blob.localhost:10010") into its account name and service
// label ("blob"/"queue"/"table"). Only the first two dot-separated labels
// are interpreted -- everything after is the host:port this listener is
// itself reachable on, which this package never needs to validate: any
// request that reaches this dedicated, single-purpose listener at all is
// presumptively meant for it.
func accountAndServiceFromHost(host string) (string, string, bool) {
	parts := strings.SplitN(host, ".", 3) //nolint:mnd // 3: account, service label, remainder
	if len(parts) != 3 || parts[0] == "" {
		return "", "", false
	}

	svc := strings.ToLower(parts[1])
	if svc != blobServiceLabel && svc != queueServiceLabel && svc != tableServiceLabel {
		return "", "", false
	}

	return parts[0], svc, true
}

// Reset is a no-op: this listener owns no state of its own to clear (see
// the package doc comment). Blob/Queue/Table each reset their own state
// independently when the POST /_gopherstack/reset endpoint calls Reset() on
// every registered service.
func (h *Handler) Reset() {}

// StartWorker binds the dedicated virtual-hosted storage listener. Mirrors
// services/azureblob's StartWorker exactly (fixed port, fail-fast, no
// fallback pool) -- see that function's doc comment for the full rationale.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurestoragevhost: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureStorageVHost", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: vhostReadHeaderTimeout,
		ReadTimeout:       vhostReadTimeout,
		IdleTimeout:       vhostIdleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azurestoragevhost", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "azurestoragevhost: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azurestoragevhost: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the dedicated virtual-hosted storage listener. Mirrors
// services/azureblob's Shutdown exactly.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "azurestoragevhost: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azurestoragevhost: forced close also failed", "error", closeErr)
		}
	}
}
