package azuremonitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	unknownOperation  = "Unknown"
	maxIngestBytes    = 1 << 20
	maxQueryBody      = 4 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 60 * time.Second
	idleTimeout       = 120 * time.Second
	opIngest          = "Upload"
	opQuery           = "Query"
	opBatch           = "QueryBatch"
	opQueryResource   = "QueryResource"
	segV1             = "v1"
	segWorkspaces     = "workspaces"
	segBatch          = "$batch"
	segQuery          = "query"
	segStreams        = "streams"
	segDCR            = "dataCollectionRules"
	pathIngestLen     = 4
	statusOK          = http.StatusOK
)

var (
	errTLSKeyMissing  = errors.New("azuremonitor: --azure-monitor-tls-cert set without --azure-monitor-tls-key")
	errTLSCertMissing = errors.New("azuremonitor: --azure-monitor-tls-key set without --azure-monitor-tls-cert")
)

// Handler serves the Logs Ingestion and Logs Query APIs on a dedicated HTTPS listener.
type Handler struct {
	Backend *Backend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	// Settings holds the listener configuration.
	Settings Settings
	// Port is the fixed TCP port StartWorker binds.
	Port int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler.
func NewHandler(backend *Backend, settings Settings) *Handler {
	backend.Configure(settings)

	return &Handler{
		Backend: backend, Settings: settings, Port: settings.Port, srvMu: lockmetrics.New("azuremonitor.server"),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AzureMonitor" }

// RouteMatcher never matches: Azure Monitor Logs runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the data-plane operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{opIngest, opQuery, opBatch, opQueryResource}
}

// ExtractOperation names the operation for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classify(c.Request())

	return op
}

// ExtractResource returns the request path for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string { return c.Request().URL.Path }

type route struct {
	op string
	// target is the DCR immutable ID, workspace ID or resource ID.
	target string
	stream string
}

func splitPath(path string) []string {
	raw := strings.Split(path, "/")
	segs := raw[:0]

	for _, s := range raw {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

func classify(r *http.Request) (string, route) {
	segs := splitPath(r.URL.EscapedPath())

	for i := range segs {
		if u, err := url.PathUnescape(segs[i]); err == nil {
			segs[i] = u
		}
	}

	switch {
	case r.Method == http.MethodPost && len(segs) == pathIngestLen && segs[0] == segDCR && segs[2] == segStreams:
		return opIngest, route{op: opIngest, target: segs[1], stream: segs[3]}
	case len(segs) == 4 && segs[0] == segV1 && segs[1] == segWorkspaces && segs[3] == segQuery &&
		(r.Method == http.MethodPost || r.Method == http.MethodGet):
		return opQuery, route{op: opQuery, target: segs[2]}
	case r.Method == http.MethodPost && len(segs) == 2 && segs[0] == segV1 && segs[1] == segBatch:
		return opBatch, route{op: opBatch}
	case len(segs) >= 3 && segs[0] == segV1 && segs[len(segs)-1] == segQuery && segs[1] != segWorkspaces &&
		(r.Method == http.MethodPost || r.Method == http.MethodGet):
		return opQueryResource, route{op: opQueryResource, target: "/" + strings.Join(segs[1:len(segs)-1], "/")}
	}

	return unknownOperation, route{op: unknownOperation}
}

func (h *Handler) authError(r *http.Request) string {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return "Valid authentication was not provided"
	}

	if !h.Settings.ValidateTokens {
		return ""
	}

	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(token, claims); err != nil {
		return "The bearer token is not a well-formed JWT"
	}

	if exp, err := claims.GetExpirationTime(); err != nil || (exp != nil && exp.Before(time.Now())) {
		return "The bearer token has expired"
	}

	return ""
}

func writeAPIError(c *echo.Context, e *apiError) error {
	return c.JSON(e.status, e.envelope(c.Response().Header().Get("X-Ms-Correlation-Request-Id")))
}

// Handler returns the Echo handler for the Azure Monitor Logs APIs.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r, w := c.Request(), c.Response()
		w.Header().Set("X-Ms-Correlation-Request-Id", uuid.NewString())
		w.Header().Set("Cache-Control", "no-store")

		op, rt := classify(r)
		if op == unknownOperation {
			return writeAPIError(c, newErr(http.StatusNotFound, "PathNotFoundError", "The requested path does not exist"))
		}

		if msg := h.authError(r); msg != "" {
			w.Header().Set("WWW-Authenticate", `Bearer authorization_uri="https://login.microsoftonline.com/common"`)

			return writeAPIError(c, errUnauthorized(msg))
		}

		switch op {
		case opIngest:
			return h.handleIngest(c, rt)
		case opBatch:
			return h.handleBatch(c)
		}

		return h.handleQuery(c, rt)
	}
}

func readBody(r *http.Request, limit int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, false, fmt.Errorf("read request body: %w", err)
	}

	return data, int64(len(data)) > limit, nil
}

func (h *Handler) handleIngest(c *echo.Context, rt route) error {
	r := c.Request()

	if r.URL.Query().Get("api-version") == "" {
		return writeAPIError(c, newErr(http.StatusBadRequest, "InvalidRequest", "The api-version query parameter is required."))
	}

	data, tooBig, err := readBody(r, maxIngestBytes)
	if err != nil {
		return writeAPIError(c, newErr(http.StatusBadRequest, "InvalidRequest", "The request body could not be read."))
	}

	if tooBig {
		return writeAPIError(c, newErr(http.StatusRequestEntityTooLarge, "ContentLengthLimitExceeded",
			"The request body exceeds the 1 MB limit."))
	}

	data, aerr := decodeContent(r.Header.Get("Content-Encoding"), data)
	if aerr != nil {
		return writeAPIError(c, aerr)
	}

	records, aerr := parseRecords(data)
	if aerr != nil {
		return writeAPIError(c, aerr)
	}

	if aerr = h.Backend.Ingest(rt.target, rt.stream, records); aerr != nil {
		return writeAPIError(c, aerr)
	}

	return c.NoContent(http.StatusNoContent)
}

func decodeContent(encoding string, data []byte) ([]byte, *apiError) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return data, nil
	case "gzip":
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, newErr(http.StatusBadRequest, "InvalidPayload", "The gzip body could not be decoded.")
		}

		defer zr.Close()

		out, err := io.ReadAll(io.LimitReader(zr, maxIngestBytes+1))
		if err != nil {
			return nil, newErr(http.StatusBadRequest, "InvalidPayload", "The gzip body could not be decoded.")
		}

		if len(out) > maxIngestBytes {
			return nil, newErr(http.StatusRequestEntityTooLarge, "ContentLengthLimitExceeded",
				"The decompressed request body exceeds the 1 MB limit.")
		}

		return out, nil
	}

	return nil, newErr(http.StatusBadRequest, "InvalidContentEncoding",
		"Content-Encoding '%s' is not supported; use gzip or none.", encoding)
}

func parseRecords(data []byte) ([]map[string]any, *apiError) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var records []map[string]any
	if err := dec.Decode(&records); err != nil {
		return nil, newErr(http.StatusBadRequest, "InvalidPayload",
			"The request body must be a JSON array of objects: %v", err)
	}

	return records, nil
}

func (h *Handler) handleQuery(c *echo.Context, rt route) error {
	r := c.Request()

	req, stats, aerr := parseQueryRequest(r)
	if aerr != nil {
		return writeAPIError(c, aerr)
	}

	var (
		resp *QueryResponse
		qerr *apiError
	)

	if rt.op == opQuery {
		resp, qerr = h.Backend.QueryWorkspace(rt.target, req, stats)
	} else {
		resp, qerr = h.Backend.QueryResource(rt.target, req, stats)
	}

	if qerr != nil {
		return writeAPIError(c, qerr)
	}

	return c.JSON(statusOK, resp)
}

func parseQueryRequest(r *http.Request) (QueryRequest, bool, *apiError) {
	var req QueryRequest

	q := r.URL.Query()

	if r.Method == http.MethodGet {
		req.Query, req.Timespan = q.Get("query"), q.Get("timespan")
	} else {
		data, tooBig, err := readBody(r, maxQueryBody)
		if err != nil || tooBig {
			return req, false, newErr(http.StatusBadRequest, "BadArgumentError", "The request body could not be read.")
		}

		if err = json.Unmarshal(data, &req); err != nil {
			return req, false, newErr(http.StatusBadRequest, "BadArgumentError",
				"The request body is not valid JSON: %v", err)
		}

		if req.Timespan == "" {
			req.Timespan = q.Get("timespan")
		}
	}

	return req, strings.Contains(r.Header.Get("Prefer"), "include-statistics=true"), nil
}

type batchRequest struct {
	Requests []batchItem `json:"requests"`
}

type batchItem struct {
	Body      QueryRequest `json:"body"`
	ID        string       `json:"id"`
	Method    string       `json:"method"`
	Path      string       `json:"path"`
	Workspace string       `json:"workspace"`
}

type batchResponse struct {
	Body    any    `json:"body"`
	ID      string `json:"id"`
	Status  int    `json:"status"`
	Headers any    `json:"headers,omitempty"`
}

func (h *Handler) handleBatch(c *echo.Context) error {
	data, tooBig, err := readBody(c.Request(), maxQueryBody)
	if err != nil || tooBig {
		return writeAPIError(c, newErr(http.StatusBadRequest, "BadArgumentError", "The request body could not be read."))
	}

	var req batchRequest
	if err = json.Unmarshal(data, &req); err != nil || len(req.Requests) == 0 {
		return writeAPIError(c, newErr(http.StatusBadRequest, "BadArgumentError",
			"The batch must contain a non-empty 'requests' array."))
	}

	out := make([]batchResponse, 0, len(req.Requests))

	for _, item := range req.Requests {
		out = append(out, h.runBatchItem(item))
	}

	return c.JSON(statusOK, map[string]any{"responses": out})
}

func (h *Handler) runBatchItem(item batchItem) batchResponse {
	fail := func(e *apiError) batchResponse {
		return batchResponse{ID: item.ID, Status: e.status, Body: e.envelope("")}
	}

	if item.Workspace == "" {
		return fail(newErr(http.StatusBadRequest, "BadArgumentError", "The batch request needs a 'workspace'."))
	}

	resp, aerr := h.Backend.QueryWorkspace(item.Workspace, item.Body, false)
	if aerr != nil {
		return fail(aerr)
	}

	return batchResponse{ID: item.ID, Status: statusOK, Body: resp}
}

func (h *Handler) loadOrGenerateCert() (tls.Certificate, error) {
	certFile, keyFile := h.Settings.TLSCertFile, h.Settings.TLSKeyFile

	switch {
	case certFile != "" && keyFile == "":
		return tls.Certificate{}, errTLSKeyMissing
	case certFile == "" && keyFile != "":
		return tls.Certificate{}, errTLSCertMissing
	case certFile != "":
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("azuremonitor: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azuremonitor: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the fixed port and serves HTTPS, failing fast if the port is taken.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azuremonitor: bind port %d: %w", h.Port, err)
	}

	cert, err := h.loadOrGenerateCert()
	if err != nil {
		_ = listener.Close()

		return err
	}

	tlsListener := tls.NewListener(
		listener, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureMonitor", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azuremonitor", "listener")
	log := logger.Load(workerCtx)
	log.InfoContext(workerCtx, "azuremonitor: starting dedicated HTTPS listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(tlsListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azuremonitor: listener stopped", "error", serveErr)
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
		log.ErrorContext(ctx, "azuremonitor: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azuremonitor: forced close also failed", "error", closeErr)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azuremonitor: restore snapshot: %w", err)
	}

	return nil
}
