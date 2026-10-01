package cognitoidp

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathOAuthToken     = "/oauth2/token" //nolint:gosec // endpoint path, not a credential
	pathOAuthAuthorize = "/oauth2/authorize"
	pathOAuthUserInfo  = "/oauth2/userInfo"
	pathOAuthRevoke    = "/oauth2/revoke"
	pathLogin          = "/login"
	pathLogout         = "/logout"
	discoverySuffix    = "/.well-known/openid-configuration"
	rootDiscoveryPath  = discoverySuffix
	rootJWKSPath       = jwksPathSuffix
	sigV4Prefix        = "AWS4-"

	opOAuthToken     = "OAuth2Token" //nolint:gosec // operation name, not a credential
	opOAuthAuthorize = "OAuth2Authorize"
	opOAuthUserInfo  = "OAuth2UserInfo"
	opOAuthRevoke    = "OAuth2Revoke"
	opHostedLogin    = "HostedLogin"
	opHostedLogout   = "HostedLogout"
	opOIDCDiscovery  = "GetOpenIDConfiguration"
	opOAuthPreflight = "OAuth2Preflight"
)

// oauthOp classifies a request as one of the hosted OAuth2/OIDC endpoints, or "" when it is not one.
// SigV4-signed and X-Amz-Target requests are never OAuth traffic, so other services keep their paths.
func (h *Handler) oauthOp(r *http.Request) string {
	if r.Header.Get("X-Amz-Target") != "" || strings.HasPrefix(r.Header.Get("Authorization"), sigV4Prefix) {
		return ""
	}

	switch r.URL.Path {
	case pathOAuthToken, pathOAuthRevoke, pathOAuthUserInfo:
		return oauthAPIOp(r)
	case pathOAuthAuthorize, pathLogin, pathLogout:
		return oauthBrowserOp(r)
	case rootDiscoveryPath, rootJWKSPath:
		return h.rootWellKnownOp(r)
	}

	if poolPathPrefix(r.URL.Path, discoverySuffix) != "" && r.Method == http.MethodGet {
		return opOIDCDiscovery
	}

	return ""
}

func oauthAPIOp(r *http.Request) string {
	if r.Method == http.MethodOptions {
		return opOAuthPreflight
	}

	switch r.URL.Path {
	case pathOAuthToken:
		return methodOp(r, http.MethodPost, opOAuthToken)
	case pathOAuthRevoke:
		return methodOp(r, http.MethodPost, opOAuthRevoke)
	default:
		if r.Method == http.MethodGet || r.Method == http.MethodPost {
			return opOAuthUserInfo
		}

		return ""
	}
}

func oauthBrowserOp(r *http.Request) string {
	if !r.URL.Query().Has("client_id") {
		return ""
	}

	switch r.URL.Path {
	case pathOAuthAuthorize:
		return methodOp(r, http.MethodGet, opOAuthAuthorize)
	case pathLogout:
		return methodOp(r, http.MethodGet, opHostedLogout)
	default:
		if r.Method == http.MethodGet || r.Method == http.MethodPost {
			return opHostedLogin
		}

		return ""
	}
}

func methodOp(r *http.Request, method, op string) string {
	if r.Method == method {
		return op
	}

	return ""
}

// rootWellKnownOp serves the pool-less /.well-known paths only on a registered user pool domain host.
func (h *Handler) rootWellKnownOp(r *http.Request) string {
	if r.Method != http.MethodGet {
		return ""
	}

	if _, ok := h.Backend.domainPoolID(r.Host); !ok {
		return ""
	}

	if r.URL.Path == rootDiscoveryPath {
		return opOIDCDiscovery
	}

	return "GetJWKS"
}

// poolPathPrefix returns the pool ID when path is exactly /<pool><suffix>, else "".
func poolPathPrefix(path, suffix string) string {
	rest, ok := strings.CutSuffix(path, suffix)
	if !ok || !strings.HasPrefix(rest, "/") {
		return ""
	}

	pool := rest[1:]
	if pool == "" || strings.Contains(pool, "/") {
		return ""
	}

	return pool
}

func (h *Handler) handleOAuth(c *echo.Context, op string) error {
	switch op {
	case opOAuthToken:
		return h.handleOAuthToken(c)
	case opOAuthRevoke:
		return h.handleOAuthRevoke(c)
	case opOAuthUserInfo:
		return h.handleOAuthUserInfo(c)
	case opOAuthAuthorize:
		return h.handleOAuthAuthorize(c)
	case opHostedLogin:
		return h.handleHostedLogin(c)
	case opHostedLogout:
		return h.handleHostedLogout(c)
	case opOIDCDiscovery:
		return h.handleOIDCDiscovery(c)
	case opOAuthPreflight:
		return handleOAuthPreflight(c)
	default:
		return h.handleJWKS(c)
	}
}
