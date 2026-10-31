package azuregraph

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	unknownOperation  = "Unknown"
	maxBodyBytes      = 4 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 60 * time.Second
	idleTimeout       = 120 * time.Second
	graphContentType  = "application/json;odata.metadata=minimal;odata.streaming=true;" +
		"IEEE754Compatible=false;charset=utf-8"
	odataDateLayout = "2006-01-02T15:04:05"
)

var (
	errTLSKeyMissing  = errors.New("azuregraph: --azure-graph-tls-cert set without --azure-graph-tls-key")
	errTLSCertMissing = errors.New("azuregraph: --azure-graph-tls-key set without --azure-graph-tls-cert")
)

// Handler serves Microsoft Graph v1.0 directory APIs on a dedicated HTTPS listener.
type Handler struct {
	Backend *Backend
	// Issuer verifies bearer tokens when Settings.ValidateTokens is set; cli wiring shares ARM's issuer.
	Issuer *aadauth.Issuer
	srvMu  *lockmetrics.RWMutex
	srv    *http.Server
	colls  map[string]*collection
	subs   map[string]subRoute
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
	var issuer *aadauth.Issuer
	if settings.ValidateTokens {
		issuer, _ = aadauth.NewIssuer()
	}

	h := &Handler{
		Backend: backend, Issuer: issuer, Settings: settings, Port: settings.Port,
		srvMu: lockmetrics.New("azuregraph.server"), colls: buildCollections(),
	}
	h.subs = h.subRoutes()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AzureGraph" }

// RouteMatcher never matches: Microsoft Graph runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// ExtractResource returns the request path for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string { return c.Request().URL.Path }

// GetSupportedOperations lists the operation names ExtractOperation can return.
func (h *Handler) GetSupportedOperations() []string {
	ops := []string{"GetMe", "GetOrganization", "ListDomains", "GetDomain", "GetByIds", "GetDirectoryObject"}

	collections := []string{collApplications, collServicePrincipals, collUsers, collGroups, "oauth2PermissionGrants"}

	for _, name := range collections {
		for _, verb := range []string{"List", "Create", "Get", "Update", "Delete", "Count"} {
			ops = append(ops, verb+upperFirst(name))
		}
	}

	return append(ops, "AddPassword", "RemovePassword", "AddKey", "RemoveKey", "ListRelation", "AddReference",
		"RemoveReference", "CreateAppRoleAssignment", "DeleteAppRoleAssignment")
}

// ExtractOperation names the operation for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	segs := graphSegments(r)

	if len(segs) == 0 {
		return unknownOperation
	}

	return operationFor(r.Method, segs)
}

func upperFirst(s string) string { return strings.ToUpper(s[:1]) + s[1:] }

func operationFor(method string, segs []string) string {
	name, key := splitKey(segs[0])
	name = upperFirst(name)
	verbs := map[string]string{http.MethodGet: "Get", http.MethodPatch: "Update", http.MethodDelete: "Delete"}

	switch {
	case name == "Me":
		return "GetMe"
	case name == "Organization":
		return "GetOrganization"
	case name == "Domains":
		return map[bool]string{true: "ListDomains", false: "GetDomain"}[len(segs) == 1]
	case name == "DirectoryObjects" && method == http.MethodPost:
		return "GetByIds"
	case name == "DirectoryObjects":
		return "GetDirectoryObject"
	case len(segs) == 1 && key.Field == "":
		return map[string]string{http.MethodGet: "List", http.MethodPost: "Create"}[method] + name
	case len(segs) == 2 && segs[1] == "$count":
		return "Count" + name
	case len(segs) == 2 || (len(segs) == 1 && key.Field != ""):
		return verbs[method] + name
	}

	return subOperation(method, segs[len(segs)-1], segs)
}

func subOperation(method, last string, segs []string) string {
	switch {
	case strings.EqualFold(last, "addPassword"):
		return "AddPassword"
	case strings.EqualFold(last, "removePassword"):
		return "RemovePassword"
	case strings.EqualFold(last, "addKey"):
		return "AddKey"
	case strings.EqualFold(last, "removeKey"):
		return "RemoveKey"
	case last == segRef && method == http.MethodPost:
		return "AddReference"
	case last == segRef:
		return "RemoveReference"
	case len(segs) > 2 && strings.HasPrefix(segs[2], "appRoleAssign") && method == http.MethodPost:
		return "CreateAppRoleAssignment"
	case len(segs) > 3 && strings.HasPrefix(segs[2], "appRoleAssign") && method == http.MethodDelete:
		return "DeleteAppRoleAssignment"
	}

	return "ListRelation"
}

// graphSegments returns the path segments after the API version, or nil for a path without one.
func graphSegments(r *http.Request) []string {
	raw := strings.Split(r.URL.Path, "/")
	segs := raw[:0]

	for _, s := range raw {
		if s != "" {
			segs = append(segs, s)
		}
	}

	if len(segs) == 0 || (segs[0] != "v1.0" && segs[0] != "beta") {
		return nil
	}

	return segs[1:]
}

func (h *Handler) writeErr(c *echo.Context, err error) error {
	var ge *Error
	if !errors.As(err, &ge) {
		ge = newErr(http.StatusInternalServerError, "generalException", "%s", "An internal error occurred.")
	}

	w := c.Response()
	w.Header().Set("Content-Type", graphContentType)

	return c.JSON(ge.Status, errorEnvelope{Error: errorBody{
		Code: ge.Code, Message: ge.Message,
		InnerError: innerError{
			Date:            time.Now().UTC().Format(odataDateLayout),
			RequestID:       w.Header().Get("Request-Id"),
			ClientRequestID: w.Header().Get("Client-Request-Id"),
		},
	}})
}

// claimsOf authenticates the bearer token and returns its claims.
func (h *Handler) claimsOf(r *http.Request) (jwt.MapClaims, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, newErr(http.StatusUnauthorized, codeInvalidAuth, "Access token is empty.")
	}

	claims := jwt.MapClaims{}

	if !h.Settings.ValidateTokens {
		if _, _, err := jwt.NewParser().ParseUnverified(token, claims); err != nil {
			return nil, newErr(
				http.StatusUnauthorized,
				codeInvalidAuth,
				"CompactToken parsing failed with error code: 80049217",
			)
		}

		return claims, nil
	}

	if h.Issuer == nil {
		return nil, newErr(
			http.StatusUnauthorized,
			codeInvalidAuth,
			"Access token validation failure. Invalid signature.",
		)
	}

	claims, err := h.Issuer.Parse(token)
	if err != nil {
		msg := "Access token validation failure. Invalid signature."
		if errors.Is(err, jwt.ErrTokenExpired) {
			msg = "Access token has expired or is not yet valid."
		}

		return nil, newErr(http.StatusUnauthorized, codeInvalidAuth, "%s", msg)
	}

	if aud, audErr := claims.GetAudience(); audErr != nil || !slices.ContainsFunc(aud, h.audienceAllowed(r)) {
		return nil, newErr(
			http.StatusUnauthorized,
			codeInvalidAuth,
			"Access token validation failure. Invalid audience.",
		)
	}

	return claims, nil
}

func (h *Handler) audienceAllowed(r *http.Request) func(string) bool {
	return func(aud string) bool {
		switch strings.TrimSuffix(aud, "/") {
		case "https://graph.microsoft.com", "https://graph.microsoft.us", "00000003-0000-0000-c000-000000000000",
			"https://" + r.Host:
			return true
		}

		return false
	}
}

// Handler returns the Echo handler for the Graph API.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r, w := c.Request(), c.Response()
		w.Header().Set("Request-Id", uuid.NewString())
		w.Header().Set("Client-Request-Id", orDefault(r.Header.Get("Client-Request-Id"), uuid.NewString()))
		w.Header().Set("Cache-Control", "no-cache")

		claims, err := h.claimsOf(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm=""`)

			return h.writeErr(c, err)
		}

		segs := graphSegments(r)
		if len(segs) == 0 {
			return h.writeErr(
				c,
				newErr(
					http.StatusBadRequest,
					codeBadSegment,
					"Invalid version: '%s'.",
					strings.TrimPrefix(r.URL.Path, "/"),
				),
			)
		}

		return h.dispatch(c, segs, claims)
	}
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
			return tls.Certificate{}, fmt.Errorf("azuregraph: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azuregraph: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the fixed port and serves HTTPS, failing fast if the port is taken.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azuregraph: bind port %d: %w", h.Port, err)
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
	e.Any("/*", telemetry.WrapEchoHandler("AzureGraph", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azuregraph", "listener")
	log := logger.Load(workerCtx)
	log.InfoContext(workerCtx, "azuregraph: starting dedicated HTTPS listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(tlsListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azuregraph: listener stopped", "error", serveErr)
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
		log.ErrorContext(ctx, "azuregraph: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azuregraph: forced close also failed", "error", closeErr)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azuregraph: restore snapshot: %w", err)
	}

	return nil
}
