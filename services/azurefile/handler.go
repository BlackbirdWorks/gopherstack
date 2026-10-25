package azurefile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// azureFileVersion is the x-ms-version the pinned azfile client sends.
const azureFileVersion = "2026-02-06"

const (
	minHostLabels     = 3
	kindFile          = "file"
	kindBlob          = "blob"
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 60 * time.Second
	idleTimeout       = 120 * time.Second
	maxBodyBytes      = 4 << 20
	unknownOperation  = "Unknown"
)

// BlobReader reads blobs from the emulator's own Azure Blob service, for
// Copy File and Put Range From URL sources.
type BlobReader interface {
	ReadBlob(container, blob, snapshot string) ([]byte, string, error)
}

// Handler serves the Azure Files REST API on a dedicated listener.
type Handler struct {
	Backend *InMemoryBackend
	// Blobs resolves blob copy sources; nil rejects them.
	Blobs BlobReader
	// Now is the clock; nil means time.Now.
	Now   func() time.Time
	srvMu *lockmetrics.RWMutex
	srv   *http.Server
	// Port is the fixed TCP port StartWorker binds.
	Port int
	// BlobPort is the Blob listener port, used to recognise blob source URLs.
	BlobPort int
	// VerifyAuth makes requests signed with the development account's key
	// (Shared Key or SAS) fail when the signature is wrong or expired.
	VerifyAuth bool
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler on DefaultPort.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, Port: DefaultPort, srvMu: lockmetrics.New("azurefile.server")}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AzureFile" }

// RouteMatcher never matches: Azure Files runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}

	return time.Now().UTC()
}

// ExtractOperation names the operation for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	rc, ok := h.newReqCtx(c.Request())
	if !ok {
		return unknownOperation
	}

	name, _ := h.route(rc)

	return name
}

// ExtractResource returns the share name for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rc, ok := h.newReqCtx(c.Request())
	if !ok {
		return ""
	}

	return rc.share
}

// reqCtx is the parsed target of a request.
type reqCtx struct {
	now       time.Time
	r         *http.Request
	q         url.Values
	acct      string
	share     string
	path      string
	endpoint  string
	pathStyle bool
}

func (rc *reqCtx) hdr(name string) string { return rc.r.Header.Get(name) }

func (rc *reqCtx) body() ([]byte, *apiError) {
	data, err := io.ReadAll(io.LimitReader(rc.r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, newErr(http.StatusBadRequest, "InvalidInput", "The request body could not be read.")
	}

	if len(data) > maxBodyBytes {
		return nil, newErr(http.StatusRequestEntityTooLarge, "RequestBodyTooLarge",
			"The size of the request body exceeds the maximum permissible limit.")
	}

	return data, nil
}

// splitHostPath returns account, share, in-share path and the endpoint path prefix: an
// "<account>.file.<host>" host selects the account, otherwise the first path
// segment is the account. prefix is the path prefix an endpoint URL needs.
func splitHostPath(hostport, urlPath string) (string, string, string, string, bool) {
	host := hostport
	if hp, _, err := net.SplitHostPort(hostport); err == nil {
		host = hp
	}

	p := strings.TrimPrefix(urlPath, "/")
	labels := strings.Split(host, ".")

	if len(labels) >= minHostLabels && strings.EqualFold(labels[1], kindFile) {
		shr, path, _ := strings.Cut(p, "/")

		return labels[0], shr, path, "", true
	}

	acct, rest, _ := strings.Cut(p, "/")
	if acct == "" {
		return "", "", "", "", false
	}

	shr, path, _ := strings.Cut(rest, "/")

	return acct, shr, path, "/" + acct, true
}

func (h *Handler) newReqCtx(r *http.Request) (*reqCtx, bool) {
	acct, shr, path, prefix, ok := splitHostPath(r.Host, r.URL.Path)
	if !ok {
		return nil, false
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	return &reqCtx{
		r: r, q: r.URL.Query(), acct: acct, share: shr, path: path, now: h.now(),
		endpoint: scheme + "://" + r.Host + prefix, pathStyle: prefix != "",
	}, true
}

// resp is a buffered response written after the backend lock is released.
type resp struct {
	hdr    http.Header
	stream io.Reader
	ctype  string
	body   []byte
	status int
}

func newResp(status int) *resp { return &resp{status: status, hdr: http.Header{}} }

func (rs *resp) xml(v any) *resp {
	body, err := xml.Marshal(v)
	if err != nil {
		return errResp(newErr(http.StatusInternalServerError, "InternalError", "Failed to marshal response."))
	}

	rs.body = append([]byte(xml.Header), body...)
	rs.ctype = "application/xml"

	return rs
}

func (rs *resp) json(b []byte) *resp {
	rs.body = b
	rs.ctype = "application/json"

	return rs
}

type azureError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func errResp(e *apiError) *resp {
	rs := newResp(e.Status)
	rs.hdr.Set("X-Ms-Error-Code", e.Code)

	return rs.xml(azureError{Code: e.Code, Message: e.Message})
}

type opFn func(*reqCtx) (*resp, *apiError)

// Handler returns the Echo handler for Azure Files requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		w := c.Response()

		hdr := w.Header()
		hdr.Set("X-Ms-Version", azureFileVersion)
		hdr.Set("X-Ms-Request-Id", newRequestID())
		hdr.Set("Date", fmtHTTP(h.now()))

		rc, ok := h.newReqCtx(r)
		if !ok {
			return h.write(c, errResp(newErr(http.StatusBadRequest, "InvalidUri",
				"The requested URI does not represent any resource on the server.")))
		}

		if aerr := h.authenticate(rc); aerr != nil {
			return h.write(c, errResp(aerr))
		}

		_, fn := h.route(rc)
		if fn == nil {
			return h.write(c, errResp(newErr(http.StatusBadRequest, "InvalidUri",
				"The requested URI does not represent any resource on the server.")))
		}

		rs, aerr := fn(rc)
		if aerr != nil {
			rs = errResp(aerr)
		}

		return h.write(c, rs)
	}
}

func (h *Handler) write(c *echo.Context, rs *resp) error {
	w := c.Response()
	hdr := w.Header()

	maps.Copy(hdr, rs.hdr)

	if rs.ctype != "" && (len(rs.body) > 0 || rs.stream != nil) {
		hdr.Set("Content-Type", rs.ctype)
	}

	if c.Request().Method == http.MethodHead {
		if hdr.Get("Content-Length") == "" {
			hdr.Set("Content-Length", strconv.Itoa(len(rs.body)))
		}

		w.WriteHeader(rs.status)

		return nil
	}

	var src io.Reader = bytes.NewReader(rs.body)

	if rs.stream != nil {
		src = rs.stream
	} else {
		hdr.Set("Content-Length", strconv.Itoa(len(rs.body)))
	}

	w.WriteHeader(rs.status)

	_, err := io.Copy(w, src)

	return err
}

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// StartWorker binds the dedicated listener; a taken port fails startup.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurefile: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureFile", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azurefile", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "azurefile: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azurefile: listener stopped", "error", serveErr)
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
		log.ErrorContext(ctx, "azurefile: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azurefile: forced close also failed", "error", closeErr)
		}
	}
}
