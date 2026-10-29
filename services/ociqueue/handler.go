package ociqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	maxBodyBytes      = 1 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = time.Minute
	idleTimeout       = 120 * time.Second
	sweepInterval     = time.Minute
	adminBodyLimit    = 1 << 20
)

// req is the per-request state handed to every operation.
type req struct {
	w    http.ResponseWriter
	r    *http.Request
	rt   route
	body []byte
}

type opFunc func(h *Handler, q *req) error

// Handler serves OCI Queue: queue administration and the messages API.
type Handler struct {
	Backend     *InMemoryBackend
	srvMu       *lockmetrics.RWMutex
	srv         *http.Server
	stop        chan struct{}
	ops         map[string]opFunc
	wg          sync.WaitGroup
	Port        int
	EnforceAuth bool
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port and EnforceAuth from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend, Port: DefaultPort, ops: buildOps(),
		srvMu: lockmetrics.New("ociqueue.server"),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OCIQueue" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher never matches: OCIQueue runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string { return parseRoute(c.Request()).op }

// ExtractResource returns the addressed resource id for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	p := parseRoute(c.Request()).params
	for _, k := range []string{"queueId", "workRequestId"} {
		if v := p[k]; v != "" {
			return v
		}
	}

	return ""
}

// Reset clears all in-memory state.
func (h *Handler) Reset() { h.Backend.Reset() }

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (q *req) header(k, v string) { q.w.Header().Set(k, v) }

func (q *req) json(status int, v any, etag string) error {
	out, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("ociqueue: encode response: %w", err)
	}

	if etag != "" {
		q.header("etag", etag)
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(status)
	_, _ = q.w.Write(out)

	return nil
}

func (q *req) query(k string) string { return q.r.URL.Query().Get(k) }

func (q *req) param(k string) string { return q.rt.params[k] }

func (q *req) ifMatch() string { return q.r.Header.Get("If-Match") }

func (q *req) listOpts() ListOpts {
	n, _ := strconv.Atoi(q.query("limit"))

	return ListOpts{SortBy: q.query("sortBy"), Order: q.query("sortOrder"), Page: q.query("page"), Limit: n}
}

func (q *req) accepted(workRequestID string) error {
	q.header("opc-work-request-id", workRequestID)
	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func (q *req) nextPage(next string) {
	if next != "" {
		q.header("opc-next-page", next)
	}
}

// endpoint returns the base URL the client addressed, so returned messages endpoints stay reachable.
func (q *req) endpoint() string {
	host := q.r.Host
	if h, port, err := net.SplitHostPort(host); err == nil {
		if ip := net.ParseIP(h); ip == nil && h != "" {
			host = h + ":" + port
		}
	}

	return "http://" + host
}

func (q *req) decode(v any) error {
	if len(q.body) == 0 {
		return errMissing("A request body is required")
	}

	if err := json.Unmarshal(q.body, v); err != nil {
		return newErr(
			http.StatusBadRequest,
			"CannotParseRequest",
			"The request body is not valid JSON for this operation",
		)
	}

	return nil
}

func (h *Handler) fail(q *req, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(q.r.Context()).ErrorContext(q.r.Context(), "ociqueue: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "InternalServerError", "An unexpected error occurred")
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(ae.Status)

	out, _ := json.Marshal(errorBody{Code: ae.Code, Message: ae.Message}) //nolint:errchkjson // static shape
	_, _ = q.w.Write(out)
}

// Handler returns the Echo handler for all OCIQueue requests.
func (h *Handler) Handler() echo.HandlerFunc { return h.handle(true) }

// Trusted returns a handler that skips request-signature verification, for the in-process dashboard proxy.
func (h *Handler) Trusted() echo.HandlerFunc { return h.handle(false) }

func (h *Handler) handle(verify bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		q := &req{w: c.Response(), r: c.Request(), rt: parseRoute(c.Request())}
		q.header("opc-request-id", strings.ToUpper(uuid.NewString()))

		if q.rt.op == opAdminKeys {
			h.adminKeys(q)

			return nil
		}

		h.Backend.Sweep()

		if err := h.serve(q, verify); err != nil {
			h.fail(q, err)
		}

		return nil
	}
}

func (h *Handler) serve(q *req, verify bool) error {
	fn, ok := h.ops[q.rt.op]
	if !ok {
		return errNoRoute()
	}

	body, err := io.ReadAll(http.MaxBytesReader(q.w, q.r.Body, maxBodyBytes))
	if err != nil {
		return newErr(http.StatusRequestEntityTooLarge, "PayloadTooLarge", "Request body is too large or unreadable")
	}

	q.body = body

	if verify {
		if _, err = ociauth.Verify(q.r, q.body, ociauth.Options{
			Registry: h.Backend.Auth, Now: h.Backend.nowUTC, Enforce: h.EnforceAuth,
		}); err != nil {
			return errAuth("The required information to complete authentication was not provided or was incorrect.")
		}
	}

	return fn(h, q)
}

type adminKeyReq struct {
	Tenancy   string `json:"tenancy"`
	User      string `json:"user"`
	PublicKey string `json:"publicKey"`
}

// adminKeys is the emulator-only control endpoint (/_gopherstack/oci/keys) for registering API
// public keys; once registered, requests signed with that keyId are cryptographically verified.
func (h *Handler) adminKeys(q *req) {
	switch q.r.Method {
	case http.MethodPost:
		var in adminKeyReq

		body, _ := io.ReadAll(io.LimitReader(q.r.Body, adminBodyLimit))
		if err := json.Unmarshal(body, &in); err != nil || in.Tenancy == "" || in.User == "" {
			h.fail(q, errParam("tenancy, user and publicKey are required"))

			return
		}

		fp, err := h.Backend.Auth.Register(in.Tenancy, in.User, in.PublicKey)
		if err != nil {
			h.fail(q, errParam(err.Error()))

			return
		}

		_ = q.json(http.StatusCreated, map[string]string{
			"keyId": ociauth.KeyID{Tenancy: in.Tenancy, User: in.User, Fingerprint: fp}.String(), "fingerprint": fp,
		}, "")
	case http.MethodGet:
		_ = q.json(http.StatusOK, h.Backend.Auth.Records(), "")
	case http.MethodDelete:
		if !h.Backend.Auth.Remove(q.r.URL.Query().Get("keyId")) {
			h.fail(q, newErr(http.StatusNotFound, "NotFound", "No such API key"))

			return
		}

		q.w.WriteHeader(http.StatusNoContent)
	default:
		h.fail(q, errNoRoute())
	}
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("ociqueue: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("OCIQueue", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}
	stop := make(chan struct{})

	h.srvMu.Lock("StartWorker")
	h.srv, h.stop = srv, stop
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "ociqueue", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "ociqueue: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "ociqueue: listener stopped", "error", serveErr)
		}
	})

	h.wg.Go(func() {
		t := time.NewTicker(sweepInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				h.Backend.Sweep()
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	})

	return nil
}

// Shutdown stops the listener and the sweeper.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, stop := h.srv, h.stop
	h.srv, h.stop = nil, nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	close(stop)

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "ociqueue: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "ociqueue: forced close also failed", "error", closeErr)
		}
	}

	h.wg.Wait()
}

// stopCh is closed when the listener shuts down, releasing parked long polls.
func (h *Handler) stopCh() <-chan struct{} {
	h.srvMu.RLock("stopCh")
	defer h.srvMu.RUnlock()

	return h.stop
}
