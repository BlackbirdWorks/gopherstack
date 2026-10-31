// Package ociartifacts emulates OCI Artifacts and Container Registry: the 20160918 control plane (container
// repositories, images, signatures, container configuration, generic repositories and artifacts), the generic
// artifact content API and an OCIR-style Docker Registry v2 endpoint with token authentication, all on one
// dedicated listener. See PARITY.md.
package ociartifacts

import (
	"bytes"
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
	readTimeout       = 10 * time.Minute
	idleTimeout       = 120 * time.Second
	adminBodyLimit    = 1 << 20
)

// req is the per-request state handed to every operation.
type req struct {
	w         http.ResponseWriter
	r         *http.Request
	rt        route
	body      []byte
	principal ociauth.Principal
}

type opFunc func(h *Handler, q *req) error

// Handler serves OCI Artifacts and Container Registry.
type Handler struct {
	Backend     *InMemoryBackend
	srvMu       *lockmetrics.RWMutex
	srv         *http.Server
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
		Backend: backend, Port: DefaultPort, ops: buildOps(), srvMu: lockmetrics.New("ociartifacts.server"),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OCIArtifacts" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher never matches: OCIArtifacts runs on its own listener.
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
	for _, k := range []string{"repositoryId", "imageId", "imageSignatureId", "artifactId"} {
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
		return fmt.Errorf("ociartifacts: encode response: %w", err)
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

func (q *req) boolQuery(k string) *bool {
	v, err := strconv.ParseBool(q.query(k))
	if err != nil {
		return nil
	}

	return &v
}

func (q *req) listOpts() ListOpts {
	n, _ := strconv.Atoi(q.query("limit"))
	subtree := q.boolQuery("compartmentIdInSubtree")

	return ListOpts{
		SortBy: q.query("sortBy"), Order: q.query("sortOrder"), Page: q.query("page"), Limit: n,
		Subtree: subtree != nil && *subtree,
	}
}

func (q *req) nextPage(next string) {
	if next != "" {
		q.header("opc-next-page", next)
	}
}

func (q *req) user() string { return q.principal.KeyID.User }

func (q *req) decode(v any) error {
	if len(q.body) == 0 {
		return errMissing("A request body is required")
	}

	dec := json.NewDecoder(bytes.NewReader(q.body))
	dec.UseNumber()

	if err := dec.Decode(v); err != nil {
		return newErr(http.StatusBadRequest, "CannotParseRequest",
			"The request body is not valid JSON for this operation")
	}

	return nil
}

func (h *Handler) fail(q *req, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(q.r.Context()).ErrorContext(q.r.Context(), "ociartifacts: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "InternalServerError", "An unexpected error occurred")
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(ae.Status)

	out, _ := json.Marshal(errorBody{Code: ae.Code, Message: ae.Message}) //nolint:errchkjson // static shape
	_, _ = q.w.Write(out)
}

// Handler returns the Echo handler for all OCIArtifacts requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		q := &req{w: c.Response(), r: c.Request(), rt: parseRoute(c.Request())}
		q.header("opc-request-id", strings.ToUpper(uuid.NewString()))

		switch q.rt.op {
		case opAdmin:
			h.admin(q)
		case opRegistry:
			h.serveRegistry(q.w, q.r)
		case opGetAccessToken:
			h.serveToken(q.w, q.r)
		default:
			if err := h.serve(q); err != nil {
				h.fail(q, err)
			}
		}

		return nil
	}
}

func (h *Handler) serve(q *req) error {
	fn, ok := h.ops[q.rt.op]
	if !ok {
		return errNoRoute()
	}

	limit := int64(maxBodyBytes)
	if q.rt.op == opPutGenericContent {
		limit = maxArtifactBytes
	}

	body, err := io.ReadAll(http.MaxBytesReader(q.w, q.r.Body, limit))
	if err != nil {
		return errTooLarge()
	}

	q.body = body

	if q.principal, err = ociauth.Verify(q.r, q.body, ociauth.Options{
		Registry: h.Backend.Auth, Now: h.Backend.nowUTC, Enforce: h.EnforceAuth,
	}); err != nil {
		return errAuth("The required information to complete authentication was not provided or was incorrect.")
	}

	return fn(h, q)
}

type adminKeyReq struct {
	Tenancy   string `json:"tenancy"`
	User      string `json:"user"`
	PublicKey string `json:"publicKey"`
}

// admin serves the emulator-only endpoints under /_gopherstack/: /oci/keys registers API public keys.
func (h *Handler) admin(q *req) {
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
		return fmt.Errorf("ociartifacts: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("OCIArtifacts", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "ociartifacts", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "ociartifacts: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "ociartifacts: listener stopped", "error", serveErr)
		}
	})

	return nil
}

// Shutdown stops the listener and releases the embedded registry.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv != nil {
		log := logger.Load(ctx)

		if err := srv.Shutdown(ctx); err != nil {
			log.ErrorContext(ctx, "ociartifacts: graceful shutdown failed, forcing close", "error", err)

			if closeErr := srv.Close(); closeErr != nil {
				log.ErrorContext(ctx, "ociartifacts: forced close also failed", "error", closeErr)
			}
		}

		h.wg.Wait()
	}

	h.Backend.Close()
}
