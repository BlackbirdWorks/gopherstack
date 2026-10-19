package azurekeyvault

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	unknownOperation   = "Unknown"
	vaultsPathPrefix   = "vaults"
	challengeResource  = "https://vault.azure.net"
	challengeTenantID  = "00000000-0000-0000-0000-000000000000"
	maxBodyBytes       = 4 << 20
	readHeaderTimeout  = 10 * time.Second
	readTimeout        = 60 * time.Second
	idleTimeout        = 120 * time.Second
	requestIDLen       = 32
	hostLabelMinLabels = 2
)

var (
	errTLSKeyMissing  = errors.New("azurekeyvault: --azure-keyvault-tls-cert set without --azure-keyvault-tls-key")
	errTLSCertMissing = errors.New("azurekeyvault: --azure-keyvault-tls-key set without --azure-keyvault-tls-cert")
)

// Handler serves the Key Vault data plane on its own HTTPS listener.
type Handler struct {
	Backend  *InMemoryBackend
	srvMu    *lockmetrics.RWMutex
	srv      *http.Server
	routes   []route
	Settings Settings
	Port     int
}

// NewHandler creates a Handler.
func NewHandler(backend *InMemoryBackend, settings Settings) *Handler {
	h := &Handler{
		Backend:  backend,
		Settings: settings,
		Port:     settings.Port,
		srvMu:    lockmetrics.New("azurekeyvault.server"),
	}
	h.routes = buildRoutes()

	return h
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "AzureKeyVault" }

// GetSupportedOperations returns the supported operation names.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.routes))

	for _, r := range h.routes {
		if !slices.Contains(ops, r.op) {
			ops = append(ops, r.op)
		}
	}

	return ops
}

// RouteMatcher never matches: Key Vault runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant since RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	_, path := splitVault(r)

	if rt, _ := matchRoute(h.routes, r.Method, splitPath(path)); rt != nil {
		return rt.op
	}

	return unknownOperation
}

// ExtractResource returns the request path for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string { return c.Request().URL.Path }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// hostParts returns the hostname without port.
func hostParts(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport
	}

	return host
}

// splitVault resolves the target vault and the remaining path: either a
// /vaults/<name> path prefix or the first label of a dotted request host.
func splitVault(r *http.Request) (string, string) {
	path := r.URL.Path
	trimmed := strings.TrimPrefix(path, "/")

	if rest, ok := strings.CutPrefix(trimmed, vaultsPathPrefix+"/"); ok {
		name, tail, _ := strings.Cut(rest, "/")
		if name != "" {
			return name, "/" + tail
		}
	}

	host := hostParts(r.Host)
	if net.ParseIP(host) == nil {
		if label, _, found := strings.Cut(host, "."); found && label != "" {
			return label, path
		}
	}

	return DefaultVaultName, path
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

// baseURL is the externally visible vault URL for the request. IDs never carry
// the /vaults/<name> prefix: the Azure SDK ID parsers assume /<type>/<name>/<version>.
func baseURL(r *http.Request) string { return "https://" + r.Host }

// challengeFor builds the WWW-Authenticate challenge. When the request host
// has a vault label, the resource is the remaining domain so the SDK's
// challenge-resource verification passes without opting out.
func challengeFor(r *http.Request) string {
	resource := challengeResource

	if _, rest, found := strings.Cut(r.Host, "."); found && net.ParseIP(hostParts(r.Host)) == nil {
		resource = "https://" + rest
	}

	return fmt.Sprintf(`Bearer authorization="https://%s/%s", resource="%s"`, r.Host, challengeTenantID, resource)
}

// authError returns a rejection message for the request, or "".
func (h *Handler) authError(r *http.Request) string {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return "Request is missing a Bearer or PoP token."
	}

	if !h.Settings.ValidateTokens {
		return ""
	}

	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(token, claims); err != nil {
		return "Token is not a well-formed JWT."
	}

	if exp, err := claims.GetExpirationTime(); err != nil || (exp != nil && exp.Before(time.Now())) {
		return "Token has expired."
	}

	return ""
}

// Handler returns the Echo handler for Key Vault operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r, w := c.Request(), c.Response()
		w.Header().Set("X-Ms-Request-Id", awsid.NewN("", requestIDLen))
		w.Header().Set("Cache-Control", "no-cache")

		if msg := h.authError(r); msg != "" {
			w.Header().Set("WWW-Authenticate", challengeFor(r))

			return writeError(c, newErr(http.StatusUnauthorized, "Unauthorized", "%s", msg))
		}

		vaultName, path := splitVault(r)

		rt, params := matchRoute(h.routes, r.Method, splitPath(path))
		if rt == nil {
			return writeError(c, newErr(http.StatusNotFound, "NotFound", "The requested operation is not supported."))
		}

		if r.URL.Query().Get("api-version") == "" {
			return writeError(c, errBadParameter("The api-version query parameter is required."))
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			return writeError(c, errBadParameter("The request body could not be read."))
		}

		rc := &reqCtx{r: r, vault: vaultName, base: baseURL(r), params: params, body: body}

		status, out, err := rt.fn(h, rc)
		if err != nil {
			return writeError(c, err)
		}

		if out == nil {
			return c.NoContent(status)
		}

		return c.JSON(status, out)
	}
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *echo.Context, err error) error {
	var kv *kvError
	if !errors.As(err, &kv) {
		kv = newErr(http.StatusInternalServerError, "InternalServerError", "%s", err.Error())
	}

	return c.JSON(kv.Status, errorEnvelope{Error: errorBody{Code: kv.Code, Message: kv.Message}})
}

// decodeBody unmarshals the request JSON into T (an empty body yields the zero value).
func decodeBody[T any](rc *reqCtx) (*T, error) {
	var in T

	if len(rc.body) == 0 {
		return &in, nil
	}

	if err := json.Unmarshal(rc.body, &in); err != nil {
		return nil, errBadParameter("The request body is not valid JSON: %s", err.Error())
	}

	return &in, nil
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
			return tls.Certificate{}, fmt.Errorf("azurekeyvault: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azurekeyvault: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the fixed port and serves HTTPS, failing fast if the port is taken.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurekeyvault: bind port %d: %w", h.Port, err)
	}

	cert, err := h.loadOrGenerateCert()
	if err != nil {
		_ = listener.Close()

		return err
	}

	tlsListener := tls.NewListener(
		listener,
		&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	)

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureKeyVault", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azurekeyvault", "listener")
	log := logger.Load(workerCtx)
	log.InfoContext(workerCtx, "azurekeyvault: starting dedicated HTTPS listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(tlsListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azurekeyvault: listener stopped", "error", serveErr)
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
		log.ErrorContext(ctx, "azurekeyvault: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azurekeyvault: forced close also failed", "error", closeErr)
		}
	}
}
