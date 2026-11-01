package gcpsql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
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

type opFunc func(c *echo.Context, rt route) error

// Handler serves the Cloud SQL Admin REST API.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	ops     map[string]opFunc
	Port    int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, Port: DefaultPort, srvMu: lockmetrics.New("gcpsql.server")}
	h.ops = map[string]opFunc{}
	h.registerInstanceOps()
	h.registerAccountOps()
	h.registerMiscOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPSQL" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, 0, len(h.ops))
	for n := range h.ops {
		names = append(names, n)
	}

	return names
}

// RouteMatcher never matches: Cloud SQL runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string { return parseRoute(c.Request()).op }

// ExtractResource returns project[/instance] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())
	if rt.instance != "" {
		return rt.project + "/" + rt.instance
	}

	return rt.project
}

// Reset removes every instance.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns the Echo handler for all Cloud SQL requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		rt := parseRoute(c.Request())

		op, ok := h.ops[rt.op]
		if !ok {
			return h.fail(c, errNotFound("Not found: %s %s", c.Request().Method, c.Request().URL.Path))
		}

		if err := op(c, rt); err != nil {
			return h.fail(c, err)
		}

		return nil
	}
}

type errorItem struct {
	Message string `json:"message"`
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
}

type errorBody struct {
	Message string      `json:"message"`
	Status  string      `json:"status"`
	Errors  []errorItem `json:"errors"`
	Code    int         `json:"code"`
}

func (h *Handler) fail(c *echo.Context, err error) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "gcpsql: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "backendError", "Internal Server Error")
	}

	return c.JSON(ae.Status, map[string]errorBody{"error": {
		Code: ae.Status, Message: ae.Message, Status: statusText(ae.Status),
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

// decode reads a JSON body into T; an empty body yields the zero value.
func decode[T any](c *echo.Context) (T, error) {
	var v T

	data, err := readBody(c)
	if err != nil {
		return v, err
	}

	if len(data) == 0 {
		return v, nil
	}

	if err = json.Unmarshal(data, &v); err != nil {
		return v, errInvalid("Invalid JSON payload received: %v", err)
	}

	return v, nil
}

// decodeContext reads {key: {...}} into typed T and the raw map for echoing in the operation.
func decodeContext[T any](c *echo.Context, key string) (T, map[string]any, error) {
	var typed T

	wrapper, err := decode[map[string]json.RawMessage](c)
	if err != nil {
		return typed, nil, err
	}

	raw := wrapper[key]
	if len(raw) == 0 {
		return typed, nil, errInvalid("Missing parameter: %s.", key)
	}

	if err = json.Unmarshal(raw, &typed); err != nil {
		return typed, nil, errInvalid("Invalid %s: %v", key, err)
	}

	var echoed map[string]any

	_ = json.Unmarshal(raw, &echoed)

	return typed, echoed, nil
}

func queryInt(c *echo.Context, key string) int {
	n, _ := strconv.Atoi(c.QueryParam(key))

	return max(n, 0)
}

func (h *Handler) reply(c *echo.Context, v any, err error) error {
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, v)
}

func listBody(kind string, items any, count int, next string) map[string]any {
	out := map[string]any{keyKind: kind}
	if count > 0 {
		out["items"] = items
	}

	if next != "" {
		out["nextPageToken"] = next
	}

	return out
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpsql: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Use(gcpenforce.EchoMiddleware(ctx, gcpenforce.Target{Host: "sqladmin.googleapis.com"}))
	e.Any("/*", telemetry.WrapEchoHandler("GCPSQL", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	workerCtx := logger.WithWorker(ctx, "gcpsql", "listener")
	log := logger.Load(workerCtx)

	h.Backend.SetRunContext(context.WithoutCancel(workerCtx))

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpsql: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpsql: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the listener and every database container.
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
		log.ErrorContext(ctx, "gcpsql: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpsql: forced close also failed", "error", closeErr)
		}
	}

	h.Backend.Close()
}
