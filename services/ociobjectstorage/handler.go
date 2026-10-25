package ociobjectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	maxBodyBytes      = 512 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 5 * time.Minute
	idleTimeout       = 120 * time.Second
	lifecycleInterval = time.Minute
	adminBodyLimit    = 1 << 20
)

// req is the per-request state handed to every operation.
type req struct {
	w         http.ResponseWriter
	r         *http.Request
	principal ociauth.Principal
	rt        route
	body      []byte
}

type opFunc func(h *Handler, q *req) error

// Handler serves the OCI Object Storage API 20160918.
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
		srvMu: lockmetrics.New("ociobjectstorage.server"),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OCIObjectStorage" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher never matches: Object Storage runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string { return parseRoute(c.Request()).op }

// ExtractResource returns bucket[/object] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())
	if rt.object != "" {
		return rt.bucket + "/" + rt.object
	}

	return rt.bucket
}

// Reset clears all in-memory state.
func (h *Handler) Reset() { h.Backend.Reset() }

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (q *req) header(k, v string) { q.w.Header().Set(k, v) }

func (q *req) json(status int, v any) error {
	out, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("ociobjectstorage: encode response: %w", err)
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(status)

	if q.r.Method != http.MethodHead {
		_, _ = q.w.Write(out)
	}

	return nil
}

func (q *req) status(code int) error {
	q.w.WriteHeader(code)

	return nil
}

func (h *Handler) fail(q *req, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(q.r.Context()).ErrorContext(q.r.Context(), "ociobjectstorage: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "InternalServerError", "An unexpected error occurred")
	}

	if ae.Status == http.StatusNotModified {
		q.w.WriteHeader(ae.Status)

		return
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(ae.Status)

	if q.r.Method != http.MethodHead {
		out, _ := json.Marshal(errorBody{Code: ae.Code, Message: ae.Message}) //nolint:errchkjson // static shape
		_, _ = q.w.Write(out)
	}
}

// Handler returns the Echo handler for all Object Storage requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		q := &req{w: c.Response(), r: c.Request(), rt: parseRoute(c.Request())}
		q.header("opc-request-id", uuid.NewString())

		if q.rt.op == opAdminKeys {
			h.adminKeys(q)

			return nil
		}

		if err := h.serve(q); err != nil {
			h.fail(q, err)
		}

		return nil
	}
}

func (h *Handler) serve(q *req) error {
	fn, ok := h.ops[q.rt.op]
	if !ok {
		return errNotFound()
	}

	body, err := io.ReadAll(http.MaxBytesReader(q.w, q.r.Body, maxBodyBytes))
	if err != nil {
		return newErr(http.StatusRequestEntityTooLarge, "PayloadTooLarge", "Request body is too large or unreadable")
	}

	q.body = body

	if err = h.authenticate(q); err != nil {
		return err
	}

	return fn(h, q)
}

func (h *Handler) authenticate(q *req) error {
	if q.rt.parToken != "" {
		use := PARRead

		switch q.rt.op {
		case opPutObject:
			use = PARWrite
		case opListObjects:
			use = PARList
		}

		return h.Backend.AuthorizePAR(q.rt.parToken, q.rt.ns, q.rt.bucket, q.rt.object, use)
	}

	p, err := ociauth.Verify(q.r, q.body, ociauth.Options{
		Registry: h.Backend.Auth, Now: h.Backend.nowUTC, Enforce: h.EnforceAuth,
	})
	if err != nil {
		if q.r.Header.Get("Authorization") == "" && h.publicRead(q) {
			return nil
		}

		return errAuth("The required information to complete authentication was not provided or was incorrect.")
	}

	q.principal = p

	return nil
}

func (h *Handler) publicRead(q *req) bool {
	if q.rt.op != opGetObject && q.rt.op != opHeadObject && q.rt.op != opListObjects {
		return false
	}

	h.Backend.mu.RLock("publicRead")
	defer h.Backend.mu.RUnlock()

	bk, ok := h.Backend.buckets[q.rt.bucket]
	if !ok || q.rt.ns != h.Backend.namespace {
		return false
	}

	if q.rt.op == opListObjects {
		return bk.PublicAccessType == accessRead
	}

	return bk.PublicAccessType == accessRead || bk.PublicAccessType == accessReadNoLs
}

func (q *req) user() string {
	if q.principal.KeyID.User != "" {
		return q.principal.KeyID.User
	}

	return ""
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
		})
	case http.MethodGet:
		_ = q.json(http.StatusOK, h.Backend.Auth.Records())
	case http.MethodDelete:
		if !h.Backend.Auth.Remove(q.r.URL.Query().Get("keyId")) {
			h.fail(q, newErr(http.StatusNotFound, "NotFound", "No such API key"))

			return
		}

		q.w.WriteHeader(http.StatusNoContent)
	default:
		h.fail(q, errNotFound())
	}
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the
// caller, then starts the lifecycle sweeper.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("ociobjectstorage: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("OCIObjectStorage", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}
	stop := make(chan struct{})

	h.srvMu.Lock("StartWorker")
	h.srv, h.stop = srv, stop
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "ociobjectstorage", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "ociobjectstorage: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "ociobjectstorage: listener stopped", "error", serveErr)
		}
	})

	h.wg.Go(func() {
		t := time.NewTicker(lifecycleInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				h.Backend.SweepLifecycle()
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	})

	return nil
}

// Shutdown stops the listener and the lifecycle sweeper.
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
		log.ErrorContext(ctx, "ociobjectstorage: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "ociobjectstorage: forced close also failed", "error", closeErr)
		}
	}

	h.wg.Wait()
}
