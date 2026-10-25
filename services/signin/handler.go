package signin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	serviceName   = "Signin"
	signingName   = "signin"
	uaMarker      = "api/signin"
	hostPrefix    = "signin."
	oauthHost     = ".oauth.signin.aws"
	matchPriority = service.PriorityPathVersioned
	opUnknown     = "Unknown"
	opAuthorize   = "Authorize"
	authorizePath = "/_gopherstack/signin/authorize"
	iamQueryKey   = "x-amz-client-auth-method"
	iamQueryValue = "iam"
	formContent   = "application/x-www-form-urlencoded"
)

//nolint:gosec // G101: operation names, not credentials
const (
	opCreateOAuth2Token                       = "CreateOAuth2Token"
	opCreateOAuth2TokenWithIAM                = "CreateOAuth2TokenWithIAM"
	opIntrospectOAuth2TokenWithIAM            = "IntrospectOAuth2TokenWithIAM"
	opRevokeOAuth2TokenWithIAM                = "RevokeOAuth2TokenWithIAM"
	opPutConsoleAuthorizationConfiguration    = "PutConsoleAuthorizationConfiguration"
	opGetConsoleAuthorizationConfiguration    = "GetConsoleAuthorizationConfiguration"
	opDeleteConsoleAuthorizationConfiguration = "DeleteConsoleAuthorizationConfiguration"
	opGetResourcePolicy                       = "GetResourcePolicy"
	opPutResourcePermissionStatement          = "PutResourcePermissionStatement"
	opDeleteResourcePermissionStatement       = "DeleteResourcePermissionStatement"
	opListResourcePermissionStatements        = "ListResourcePermissionStatements"
)

// postPaths maps each fixed POST path to its operation (serializers.go opPath).
//
//nolint:gochecknoglobals // static route table
var postPaths = map[string]string{
	"/v1/introspect": opIntrospectOAuth2TokenWithIAM,
	"/v1/revoke":     opRevokeOAuth2TokenWithIAM,
	"/put-console-authorization-configuration":    opPutConsoleAuthorizationConfiguration,
	"/get-console-authorization-configuration":    opGetConsoleAuthorizationConfiguration,
	"/delete-console-authorization-configuration": opDeleteConsoleAuthorizationConfiguration,
	"/get-resource-policy":                        opGetResourcePolicy,
	"/put-resource-permission-statement":          opPutResourcePermissionStatement,
	"/delete-resource-permission-statement":       opDeleteResourcePermissionStatement,
	"/list-resource-permission-statements":        opListResourcePermissionStatements,
}

func routeOf(r *http.Request) string {
	path := r.URL.Path

	if path == authorizePath && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
		return opAuthorize
	}

	if r.Method != http.MethodPost {
		return opUnknown
	}

	if path == "/v1/token" {
		if r.URL.Query().Get(iamQueryKey) == iamQueryValue {
			return opCreateOAuth2TokenWithIAM
		}

		return opCreateOAuth2Token
	}

	if op, ok := postPaths[path]; ok {
		return op
	}

	return opUnknown
}

type dispatchFunc func(h *Handler, ctx context.Context, body []byte, r *http.Request) (int, any, error)

// Handler is the Echo handler for the Sign-In REST-JSON API.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]dispatchFunc
}

// NewHandler creates a handler over backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, ops: operationTable()}
}

// Name returns the service name.
func (h *Handler) Name() string { return serviceName }

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the implemented SDK operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// RouteMatcher matches the Sign-In paths. They carry no service prefix and the token calls are
// sent unsigned, so the SigV4 signing name, the SDK user agent or the sign-in host is required.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		switch routeOf(r) {
		case opUnknown:
			return false
		case opAuthorize:
			return true
		}

		host := strings.ToLower(r.Host)

		return httputils.ExtractServiceFromRequest(r) == signingName ||
			service.MatchesUserAgentMarker(r.Header, uaMarker) ||
			strings.HasPrefix(host, hostPrefix) || strings.Contains(host, oauthHost)
	}
}

// ExtractOperation returns the operation name for the request.
func (h *Handler) ExtractOperation(c *echo.Context) string { return routeOf(c.Request()) }

// ExtractResource returns the primary resource identifier (none for Sign-In).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		op := routeOf(r)

		ctx := r.Context()
		if _, ok := awsmeta.Key.Get(ctx); !ok {
			ctx = awsmeta.Set(ctx, awsmeta.FromRequest(r, h.Backend.Region()))
		}

		if op == opAuthorize {
			return h.authorize(ctx, c)
		}

		fn, ok := h.ops[op]
		if !ok {
			return writeError(c, notFound("unknown operation for "+r.Method+" "+r.URL.Path))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return writeError(c, validation("failed to read request body"))
		}

		logger.Load(ctx).Debug("signin request", "operation", op)

		status, out, err := fn(h, ctx, body, r)
		if err != nil {
			return writeError(c, err)
		}

		if out == nil {
			return c.NoContent(status)
		}

		return c.JSON(status, out)
	}
}

func (h *Handler) authorize(ctx context.Context, c *echo.Context) error {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return writeError(c, validation("malformed request"))
	}

	q := r.Form

	code, err := h.Backend.IssueAuthorizationCode(
		ctx, q.Get("client_id"), q.Get("redirect_uri"), q.Get("code_challenge"), q.Get("code_challenge_method"),
	)
	if err != nil {
		return writeError(c, err)
	}

	loc, err := url.Parse(q.Get("redirect_uri"))
	if err != nil || !isLoopbackRedirect(loc) {
		return writeError(c, validation("redirect_uri must be an http loopback URL"))
	}

	vals := loc.Query()
	vals.Set("code", code)

	if state := q.Get("state"); state != "" {
		vals.Set("state", state)
	}

	loc.RawQuery = vals.Encode()

	return c.Redirect(http.StatusFound, loc.String())
}

func writeError(c *echo.Context, err error) error {
	code, status := classify(err)
	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, errorWire{Error: oauthCode(err), Message: err.Error()})
}

func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return validation("malformed request body")
	}

	return nil
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
