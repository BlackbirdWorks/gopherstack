package gcpcompute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	maxBodyBytes      = 8 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 5 * time.Minute
	idleTimeout       = 120 * time.Second
)

type request struct {
	ctx  context.Context
	q    url.Values
	rt   reqRoute
	body []byte
}

type handlerFn func(h *Handler, rq *request) (any, error)

// Handler serves the Compute Engine v1 REST API on a dedicated listener.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	ops     map[string]handlerFn
	addr    string
	Port    int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{
		Backend: backend, Port: DefaultPort, srvMu: lockmetrics.New("gcpcompute.server"), ops: map[string]handlerFn{},
	}
	h.registerNetworkOps()
	h.registerAddressOps()
	h.registerStorageOps()
	h.registerInstanceOps()
	h.registerCatalogOps()
	h.registerGroupOps()
	h.registerSecurityOps()
	h.registerResourcePolicyOps()

	return h
}

func opKey(collection, scope, verb string) string { return collection + "@" + scope + "." + verb }

func (h *Handler) reg(collection, scope, verb string, fn handlerFn) {
	h.ops[opKey(collection, scope, verb)] = fn
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPCompute" }

// GetSupportedOperations lists the supported operations as collection@scope.verb.
func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, 0, len(h.ops))
	for n := range h.ops {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

// RouteMatcher never matches: Compute Engine runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	rt, ok := parseRequest(c.Request().URL.EscapedPath())
	if !ok {
		return "Unknown"
	}

	return opKey(rt.collection, rt.scope, rt.verb(c.Request().Method))
}

// ExtractResource returns the project for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt, _ := parseRequest(c.Request().URL.EscapedPath())

	return rt.project
}

// Addr returns the bound listener address, or "" before StartWorker.
func (h *Handler) Addr() string {
	h.srvMu.RLock("Addr")
	defer h.srvMu.RUnlock()

	return h.addr
}

// Reset removes all state.
func (h *Handler) Reset() { h.Backend.Reset() }

var readVerbs = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
	verbList: true, verbAggList: true, verbGet: true, "getRouterStatus": true, "getFromFamily": true,
	"listPeeringRoutes": true, "getRule": true, "serialPort": true, "getGuestAttributes": true,
}

var verbMethods = map[string]string{ //nolint:gochecknoglobals // read-only lookup table
	verbInsert: http.MethodPost, verbDelete: http.MethodDelete, verbPatch: http.MethodPatch, verbUpdate: http.MethodPut,
	"updatePeering": http.MethodPatch, "updateShieldedInstanceConfig": http.MethodPatch,
	"setShieldedInstanceIntegrityPolicy": http.MethodPatch,
}

func methodFor(verb string) string {
	if readVerbs[verb] {
		return http.MethodGet
	}

	if m, ok := verbMethods[verb]; ok {
		return m
	}

	return http.MethodPost
}

func (h *Handler) lookup(rt reqRoute, method string) (handlerFn, *APIError) {
	verb := rt.verb(method)

	scope := rt.scope
	if rt.aggregated {
		scope = "agg"
	}

	fn, ok := h.ops[opKey(rt.collection, scope, verb)]
	if !ok || methodFor(verb) != method {
		return nil, newErr(http.StatusNotFound, "notFound", "Not found: %s %s", method, rt.collection)
	}

	return fn, nil
}

func checkLocation(rt reqRoute) *APIError {
	switch rt.scope {
	case scopeZone:
		if !zoneExists(rt.loc) {
			return errNotFound(scopePath(rt.project, scopeZone, rt.loc))
		}
	case scopeRegion:
		if _, ok := findRegion(rt.loc); !ok {
			return errNotFound(scopePath(rt.project, scopeRegion, rt.loc))
		}
	}

	return nil
}

// Handler returns the Echo handler for all Compute Engine REST requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		rt, ok := parseRequest(r.URL.EscapedPath())
		if !ok {
			return h.fail(c, newErr(http.StatusNotFound, "notFound", "Not found: %s %s", r.Method, r.URL.Path))
		}

		fn, apiErr := h.lookup(rt, r.Method)
		if apiErr != nil {
			return h.fail(c, apiErr)
		}

		if apiErr = checkLocation(rt); apiErr != nil {
			return h.fail(c, apiErr)
		}

		body, err := readBody(c)
		if err != nil {
			return h.fail(c, err)
		}

		out, err := fn(h, &request{ctx: r.Context(), q: r.URL.Query(), rt: rt, body: body})
		if err != nil {
			return h.fail(c, err)
		}

		if out == nil {
			return c.NoContent(http.StatusNoContent)
		}

		return c.JSON(http.StatusOK, out)
	}
}

type errorItem struct {
	Message string `json:"message"`
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
}

type errorBody struct {
	Message string      `json:"message"`
	Errors  []errorItem `json:"errors"`
	Code    int         `json:"code"`
}

func (h *Handler) fail(c *echo.Context, err error) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "gcpcompute: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "backendError", "Internal Error")
	}

	return c.JSON(ae.Status, map[string]errorBody{"error": {
		Code: ae.Status, Message: ae.Message,
		Errors: []errorItem{{Message: ae.Message, Domain: "global", Reason: ae.Reason}},
	}})
}

func readBody(c *echo.Context) ([]byte, error) {
	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))
	if err != nil {
		return nil, newErr(http.StatusRequestEntityTooLarge, "invalid", "Request body is too large or unreadable.")
	}

	return data, nil
}

// decode reads the request body into T and also returns the raw object for presence checks and merge patches.
func decode[T any](rq *request) (*T, map[string]any, *APIError) {
	v := new(T)
	raw := map[string]any{}

	if len(strings.TrimSpace(string(rq.body))) == 0 {
		return v, raw, nil
	}

	if err := json.Unmarshal(rq.body, v); err != nil {
		return nil, nil, errInvalid("Invalid JSON payload received. %v", err)
	}

	if err := json.Unmarshal(rq.body, &raw); err != nil {
		return nil, nil, errInvalid("Invalid JSON payload received. %v", err)
	}

	return v, raw, nil
}

// StartWorker binds the REST listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	workerCtx := logger.WithWorker(ctx, "gcpcompute", "listener")
	log := logger.Load(workerCtx)

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpcompute: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(log))
	e.Any("/*", telemetry.WrapEchoHandler("GCPCompute", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv, h.addr = srv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpcompute: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpcompute: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the listener.
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
		log.ErrorContext(ctx, "gcpcompute: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpcompute: forced close also failed", "error", closeErr)
		}
	}
}
