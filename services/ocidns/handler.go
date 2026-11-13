// Package ocidns emulates OCI DNS (API 20180115): zones, records, views, resolvers and endpoints, TSIG keys and
// steering policies on a dedicated control-plane listener, plus an authoritative DNS server for GLOBAL zones.
// See PARITY.md.
package ocidns

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	pkgdns "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	maxBodyBytes      = 4 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 2 * time.Minute
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

// Handler serves OCI DNS.
type Handler struct {
	Backend      *InMemoryBackend
	srvMu        *lockmetrics.RWMutex
	srv          *http.Server
	dnsSrv       *pkgdns.Server
	ops          map[string]opFunc
	wg           sync.WaitGroup
	Port         int
	DNSPort      int
	EnforceAuth  bool
	DNSEphemeral bool
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override the ports and EnforceAuth from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend, Port: DefaultPort, DNSPort: DefaultDNSPort, ops: buildOps(),
		srvMu: lockmetrics.New("ocidns.server"),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OCIDNS" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher never matches: OCIDNS runs on its own listener.
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
	for _, k := range []string{
		"zoneNameOrId", "zoneId", "viewId", "resolverId", "tsigKeyId", "steeringPolicyId", "steeringPolicyAttachmentId",
	} {
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

func (q *req) write(status int, v any, t tag) error {
	out, err := json.Marshal(v, jsonx.V1())
	if err != nil {
		return fmt.Errorf("ocidns: encode response: %w", err)
	}

	if t.ETag != "" {
		q.header("etag", t.ETag)
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(status)
	_, _ = q.w.Write(out)

	return nil
}

func (q *req) query(k string) string { return q.r.URL.Query().Get(k) }

func (q *req) param(k string) string { return q.rt.params[k] }

func (q *req) cond() cond {
	c := cond{IfMatch: q.r.Header.Get("If-Match")}

	if t, err := http.ParseTime(q.r.Header.Get("If-Unmodified-Since")); err == nil {
		c.Unmodified = t
	}

	return c
}

func (q *req) listOpts() ListOpts {
	n, _ := strconv.Atoi(q.query("limit"))

	return ListOpts{SortBy: q.query("sortBy"), Order: q.query("sortOrder"), Page: q.query("page"), Limit: n}
}

func (q *req) timeFilter() func(time.Time) bool {
	ge, lt := q.query("timeCreatedGreaterThanOrEqualTo"), q.query("timeCreatedLessThan")
	if ge == "" && lt == "" {
		return nil
	}

	geT, _ := time.Parse(time.RFC3339, ge)
	ltT, _ := time.Parse(time.RFC3339, lt)

	return func(t time.Time) bool {
		return (ge == "" || !t.Before(geT)) && (lt == "" || t.Before(ltT))
	}
}

func (q *req) decode(v any) error {
	if len(q.body) == 0 {
		return errMissing("A request body is required")
	}

	if err := json.UnmarshalDecode(jsontext.NewDecoder(bytes.NewReader(q.body)), v, jsonx.V1()); err != nil {
		return newErr(http.StatusBadRequest, "CannotParseRequest",
			"The request body is not valid JSON for this operation")
	}

	return nil
}

func (h *Handler) fail(q *req, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(q.r.Context()).ErrorContext(q.r.Context(), "ocidns: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "InternalServerError", "An unexpected error occurred")
	}

	q.header("Content-Type", "application/json")
	q.w.WriteHeader(ae.Status)

	out, _ := json.Marshal(
		errorBody{Code: ae.Code, Message: ae.Message},
		jsonx.V1(),
	) //nolint:errchkjson // static shape
	_, _ = q.w.Write(out)
}

// Handler returns the Echo handler for all OCIDNS requests.
func (h *Handler) Handler() echo.HandlerFunc { return h.handle(true) }

// Trusted returns a handler that skips request-signature verification.
func (h *Handler) Trusted() echo.HandlerFunc { return h.handle(false) }

func (h *Handler) handle(verify bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		q := &req{w: c.Response(), r: c.Request(), rt: parseRoute(c.Request())}
		q.header("opc-request-id", strings.ToUpper(uuid.NewString()))

		if q.rt.op == opAdmin {
			h.admin(q)

			return nil
		}

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
		return errTooLarge()
	}

	q.body = body

	if q.principal, err = ociauth.Verify(q.r, q.body, ociauth.Options{
		Registry: h.Backend.Auth, Now: h.Backend.nowUTC, Enforce: h.EnforceAuth && verify,
	}); err != nil {
		return errAuth("The required information to complete authentication was not provided or was incorrect.")
	}

	h.Backend.SyncVcns()

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
		if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil || in.Tenancy == "" || in.User == "" {
			h.fail(q, errParam("tenancy, user and publicKey are required"))

			return
		}

		fp, err := h.Backend.Auth.Register(in.Tenancy, in.User, in.PublicKey)
		if err != nil {
			h.fail(q, errParam(err.Error()))

			return
		}

		_ = q.write(http.StatusCreated, map[string]string{
			"keyId": ociauth.KeyID{Tenancy: in.Tenancy, User: in.User, Fingerprint: fp}.String(), "fingerprint": fp,
		}, tag{})
	case http.MethodGet:
		_ = q.write(http.StatusOK, h.Backend.Auth.Records(), tag{})
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

// StartWorker binds the DNS server and the control-plane listener synchronously so a conflict is returned.
func (h *Handler) StartWorker(ctx context.Context) error {
	workerCtx := logger.WithWorker(ctx, "ocidns", "listener")
	log := logger.Load(workerCtx)

	var dnsSrv *pkgdns.Server

	if h.DNSPort > 0 || h.DNSEphemeral {
		var err error

		if dnsSrv, err = h.startDNS(workerCtx); err != nil {
			return err
		}
	}

	listener, port, err := portalloc.ListenTCP(ctx, h.Port)
	if err != nil {
		if dnsSrv != nil {
			_ = dnsSrv.Stop()
		}

		return fmt.Errorf("ocidns: bind port %d: %w", h.Port, err)
	}

	h.Port = port
	h.Backend.SetSelfBase(fmt.Sprintf("http://localhost:%d/%s", port, Version))

	e := echo.New()
	e.Use(logger.EchoMiddleware(log))
	e.Any("/*", telemetry.WrapEchoHandler("OCIDNS", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv, h.dnsSrv = srv, dnsSrv
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "ocidns: starting dedicated listener", "port", h.Port, "dnsPort", h.DNSPort)

	h.wg.Go(func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "ocidns: listener stopped", "error", serveErr)
		}
	})

	return nil
}

func (h *Handler) startDNS(ctx context.Context) (*pkgdns.Server, error) {
	port := h.DNSPort
	if h.DNSEphemeral {
		port = 0
	}

	srv, err := pkgdns.New(pkgdns.Config{ListenAddr: fmt.Sprintf(":%d", port), Logger: logger.Load(ctx)})
	if err != nil {
		return nil, fmt.Errorf("ocidns: dns server: %w", err)
	}

	srv.SetInterceptor(authority{b: h.Backend})

	if err = srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("ocidns: bind dns port %d: %w", h.DNSPort, err)
	}

	if h.DNSEphemeral {
		h.DNSPort = srv.Port()
	}

	return srv, nil
}

// Shutdown stops both listeners.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, dnsSrv := h.srv, h.dnsSrv
	h.srv, h.dnsSrv = nil, nil
	h.srvMu.Unlock()

	log := logger.Load(ctx)

	if dnsSrv != nil {
		if err := dnsSrv.Stop(); err != nil {
			log.ErrorContext(ctx, "ocidns: dns server stop failed", "error", err)
		}
	}

	if srv != nil {
		if err := srv.Shutdown(ctx); err != nil {
			log.ErrorContext(ctx, "ocidns: graceful shutdown failed, forcing close", "error", err)

			if closeErr := srv.Close(); closeErr != nil {
				log.ErrorContext(ctx, "ocidns: forced close also failed", "error", closeErr)
			}
		}

		h.wg.Wait()
	}
}
