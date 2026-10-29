package gcpidentitytoolkit

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
	idtkv2 "google.golang.org/api/identitytoolkit/v2"

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
	keyCacheSeconds   = 3600
)

// Handler serves the Identity Toolkit and Secure Token REST APIs on a dedicated listener.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	wg      *sync.WaitGroup
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
	return &Handler{
		Backend: backend, Port: DefaultPort, srvMu: lockmetrics.New("gcpidentitytoolkit.server"), wg: &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPIdentityToolkit" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := []string{
		"AdminCreate", "BatchGet", "CreateSessionCookie", "Token", "PublicKeys", "GetConfig", "UpdateConfig",
		"ListTenants", "CreateTenant", "GetTenant", "UpdateTenant", "DeleteTenant", "ListOobCodes", "ClearAccounts",
	}

	for _, v := range accountVerbs {
		ops = append(ops, v)
	}

	return ops
}

// RouteMatcher never matches: Identity Platform runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op := parseRoute(c.Request()).op; op != "" {
		return op
	}

	return "Unknown"
}

// ExtractResource returns project[/tenant] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())
	if rt.tenant != "" {
		return rt.project + "/" + rt.tenant
	}

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

func (h *Handler) fail(c *echo.Context, err error) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(), "gcpidentitytoolkit: internal error", "error", err)

		ae = &APIError{HTTP: http.StatusInternalServerError, Status: "INTERNAL", Message: "Internal error encountered."}
	}

	return c.JSON(ae.HTTP, ae.body())
}

func bearer(r *http.Request) bool {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")

	return ok && strings.TrimSpace(tok) != ""
}

func (h *Handler) call(c *echo.Context, rt route) call {
	project := rt.project
	if project == "" {
		project = h.Backend.defaultProjectName()
	}

	return call{project: project, tenant: rt.tenant, host: c.Request().Host, admin: bearer(c.Request())}
}

func (b *InMemoryBackend) defaultProjectName() string {
	b.mu.RLock("defaultProjectName")
	defer b.mu.RUnlock()

	return b.defaultProject
}

func readJSON[T any](c *echo.Context) (*T, map[string]any, error) {
	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))
	if err != nil {
		return nil, nil, errInvalidArg("Request body is too large or unreadable.")
	}

	v := new(T)
	raw := map[string]any{}

	if strings.TrimSpace(string(data)) == "" {
		return v, raw, nil
	}

	if err = json.Unmarshal(data, v); err != nil {
		return nil, nil, errInvalidArg("Invalid JSON payload received. %v", err)
	}

	if err = json.Unmarshal(data, &raw); err != nil {
		return nil, nil, errInvalidArg("Invalid JSON payload received. %v", err)
	}

	return v, raw, nil
}

// Handler returns the Echo handler for every Identity Toolkit and Secure Token request.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		rt := parseRoute(c.Request())
		if rt.op == "" {
			return h.fail(c, errNotFound("NOT_FOUND"))
		}

		out, err := h.dispatch(c, rt)
		if err != nil {
			return h.fail(c, err)
		}

		if out == nil {
			return c.NoContent(http.StatusNoContent)
		}

		c.Response().Header().Set("Cache-Control", "no-store")

		return c.JSON(http.StatusOK, out)
	}
}


func (h *Handler) dispatch(c *echo.Context, rt route) (any, error) {
	cl := h.call(c, rt)

	switch rt.op {
	case "Token":
		return h.token(c)
	case "PublicKeys":
		return h.publicKeys(c)
	case "ListOobCodes", "ClearAccounts":
		return h.emulatorOp(rt, cl)
	}

	if (rt.version == "v2" || slices.Contains(adminOps, rt.op)) && !cl.admin {
		return nil, errUnauth()
	}

	if !cl.admin && c.QueryParam("key") == "" && c.Request().Header.Get("X-Goog-Api-Key") == "" {
		return nil, errNoKey()
	}

	switch rt.version {
	case "v2":
		return h.adminV2(c, rt)
	}

	return h.accountOp(c, rt, cl)
}

//nolint:gochecknoglobals // immutable lookup table
var adminOps = []string{"AdminCreate", "BatchGet", "batchCreate", "batchDelete", "query", "CreateSessionCookie"}

func (h *Handler) emulatorOp(rt route, cl call) (any, error) {
	switch rt.op {
	case "ListOobCodes":
		codes, err := h.Backend.OOBCodes(cl.project, rt.tenant)
		if err != nil {
			return nil, err
		}

		return map[string]any{"oobCodes": codes}, nil
	default:
		if err := h.Backend.ClearAccounts(cl.project, rt.tenant); err != nil {
			return nil, err
		}

		return map[string]any{}, nil
	}
}

func adminGuard(cl call) error {
	if !cl.admin {
		return errUnauth()
	}

	return nil
}

func (h *Handler) accountOp(c *echo.Context, rt route, cl call) (any, error) {
	switch rt.op {
	case "BatchGet":
		return h.batchGet(c, cl)
	case "batchCreate":
		if err := adminGuard(cl); err != nil {
			return nil, err
		}

		req, _, err := readJSON[uploadRequest](c)
		if err != nil {
			return nil, err
		}

		return h.Backend.BatchCreate(cl, req)
	case "batchDelete":
		if err := adminGuard(cl); err != nil {
			return nil, err
		}

		req, _, err := readJSON[struct {
			TenantID string   `json:"tenantId"`
			LocalIDs []string `json:"localIds"`
			Force    bool     `json:"force"`
		}](c)
		if err != nil {
			return nil, err
		}

		return h.Backend.BatchDelete(cl, req.LocalIDs, req.Force, req.TenantID)
	case "query":
		if err := adminGuard(cl); err != nil {
			return nil, err
		}

		req, _, err := readJSON[queryRequest](c)
		if err != nil {
			return nil, err
		}

		return h.Backend.Query(cl, req)
	}

	req, _, err := readJSON[authRequest](c)
	if err != nil {
		return nil, err
	}

	return h.authOp(rt.op, cl, req)
}

func (h *Handler) authOp(op string, cl call, req *authRequest) (any, error) {
	b := h.Backend

	switch op {
	case "signUp":
		return b.SignUp(cl, req)
	case "AdminCreate":
		return b.AdminCreate(cl, req)
	case "signInWithPassword":
		return b.SignInWithPassword(cl, req)
	case "signInWithCustomToken":
		return b.SignInWithCustomToken(cl, req)
	case "signInWithEmailLink":
		return b.SignInWithEmailLink(cl, req)
	case "lookup":
		return b.Lookup(cl, req)
	case "update":
		return b.Update(cl, req)
	case "delete":
		if err := b.DeleteAccount(cl, req); err != nil {
			return nil, err
		}

		return map[string]any{"kind": "identitytoolkit#DeleteAccountResponse"}, nil
	case "sendOobCode":
		return b.SendOobCode(cl, req)
	case "resetPassword":
		return b.ResetPassword(cl, req)
	case "createAuthUri":
		return b.CreateAuthURI(cl, req)
	case "CreateSessionCookie":
		return b.CreateSessionCookie(cl, req)
	}

	return nil, errNotFound("NOT_FOUND")
}

func (h *Handler) batchGet(c *echo.Context, cl call) (any, error) {
	if err := adminGuard(cl); err != nil {
		return nil, err
	}

	max := 0

	if raw := c.QueryParam("maxResults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, idtk("INVALID_MAX_RESULTS")
		}

		max = n
	}

	return h.Backend.BatchGet(cl, c.QueryParam("tenantId"), c.QueryParam("nextPageToken"), max)
}

func (h *Handler) adminV2(c *echo.Context, rt route) (any, error) {
	b := h.Backend

	switch rt.op {
	case "GetConfig":
		return b.GetConfig(rt.project), nil
	case "UpdateConfig":
		_, raw, err := readJSON[idtkv2.GoogleCloudIdentitytoolkitAdminV2Config](c)
		if err != nil {
			return nil, err
		}

		return b.UpdateConfig(rt.project, raw, c.QueryParam("updateMask"))
	case "CreateTenant":
		in, _, err := readJSON[idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant](c)
		if err != nil {
			return nil, err
		}

		return b.CreateTenant(rt.project, in)
	case "GetTenant":
		return b.GetTenant(rt.project, rt.id)
	case "UpdateTenant":
		_, raw, err := readJSON[idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant](c)
		if err != nil {
			return nil, err
		}

		return b.UpdateTenant(rt.project, rt.id, raw, c.QueryParam("updateMask"))
	case "DeleteTenant":
		if err := b.DeleteTenant(rt.project, rt.id); err != nil {
			return nil, err
		}

		return map[string]any{}, nil
	case "ListTenants":
		size := 0

		if raw := c.QueryParam("pageSize"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 0 {
				return nil, errInvalidArg("Invalid page size.")
			}

			size = n
		}

		pg, err := b.ListTenants(rt.project, c.QueryParam("pageToken"), size)
		if err != nil {
			return nil, err
		}

		return &idtkv2.GoogleCloudIdentitytoolkitAdminV2ListTenantsResponse{Tenants: pg.Data, NextPageToken: pg.Next}, nil
	}

	return nil, errNotFound("NOT_FOUND")
}

func (h *Handler) token(c *echo.Context) (any, error) {
	var grant, refresh string

	if strings.HasPrefix(c.Request().Header.Get("Content-Type"), "application/json") {
		req, _, err := readJSON[struct {
			GrantType    string `json:"grant_type"`
			RefreshToken string `json:"refresh_token"`
		}](c)
		if err != nil {
			return nil, err
		}

		grant, refresh = req.GrantType, req.RefreshToken
	} else {
		if err := c.Request().ParseForm(); err != nil {
			return nil, errInvalidArg("Invalid form body.")
		}

		grant, refresh = c.Request().PostForm.Get("grant_type"), c.Request().PostForm.Get("refresh_token")
	}

	out, err := h.Backend.Refresh(grant, refresh)
	if err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == "" {
			return nil, &APIError{HTTP: ae.HTTP, Message: ae.Message, Status: "INVALID_ARGUMENT"}
		}

		return nil, err
	}

	return out, nil
}

func (h *Handler) publicKeys(c *echo.Context) (any, error) {
	kid, cert, pub, err := h.Backend.publicKeyCert()
	if err != nil {
		return nil, err
	}

	c.Response().Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", keyCacheSeconds))

	if strings.Contains(c.Request().URL.Path, "/jwk/") {
		return map[string]any{"keys": []map[string]string{jwkOf(kid, pub)}}, nil
	}

	return map[string]string{kid: cert}, nil
}

func jwkOf(kid string, pub *rsa.PublicKey) map[string]string {
	return map[string]string{
		"kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid,
		"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpidentitytoolkit: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpidentitytoolkit", "listener")
	log := logger.Load(workerCtx)

	e := echo.New()
	e.Use(logger.EchoMiddleware(log))
	e.Any("/*", telemetry.WrapEchoHandler("GCPIdentityToolkit", h.Handler(), h))

	srv := &http.Server{Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout}

	h.srvMu.Lock("StartWorker")
	h.srv, h.addr = srv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpidentitytoolkit: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpidentitytoolkit: listener stopped", "error", serveErr)
		}
	})

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
		log.ErrorContext(ctx, "gcpidentitytoolkit: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpidentitytoolkit: forced close also failed", "error", closeErr)
		}
	}

	h.wg.Wait()
}
