package azurearm

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// ClientAuthenticator checks client-credentials requests against a directory of registered applications.
// known is false for a client the directory does not know, which keeps the permissive dev-client behaviour.
type ClientAuthenticator interface {
	AuthenticateClient(clientID, secret string) (objectID string, known bool, err error)
}

// ClientAuthError is a token endpoint rejection rendered in Entra's error shape.
type ClientAuthError struct {
	Name        string
	Description string
	Status      int
	Code        int
}

func (e *ClientAuthError) Error() string { return e.Name + ": " + e.Description }

// PrincipalDirectory resolves role assignment principals; PrincipalType returns User, Group or ServicePrincipal.
type PrincipalDirectory interface {
	PrincipalType(id string) (string, bool)
}

// SetClientAuthenticator makes the token endpoint verify secrets of applications the directory knows.
// Call before the listener starts.
func (h *Handler) SetClientAuthenticator(a ClientAuthenticator) { h.clients = a }

// SetPrincipalDirectory lets role assignments resolve principals against dir; with
// Settings.ValidatePrincipals set, unknown principals are rejected with PrincipalNotFound.
func (h *Handler) SetPrincipalDirectory(dir PrincipalDirectory) {
	if p, ok := h.Registry.ProviderNamed(namespaceMicrosoftAuthorization); ok {
		if ap, isAuth := p.(*AuthorizationProvider); isAuth {
			ap.SetPrincipalDirectory(dir, h.Settings.ValidatePrincipals)
		}
	}
}

// SetPrincipalDirectory sets the directory used to type and, when validate is set, verify principals.
func (p *AuthorizationProvider) SetPrincipalDirectory(dir PrincipalDirectory, validate bool) {
	p.mu.Lock("SetPrincipalDirectory")
	defer p.mu.Unlock()

	p.directory, p.validatePrincipals = dir, validate
}

// principalKnown reports whether id is a directory object or user-assigned managed identity.
func (p *AuthorizationProvider) principalKnown(id string) bool {
	if p.directory != nil {
		if _, ok := p.directory.PrincipalType(id); ok {
			return true
		}
	}

	return p.msi != nil && p.msi.hasPrincipal(id)
}

func (p *AuthorizationProvider) directoryType(id string) (string, bool) {
	if p.directory == nil {
		return "", false
	}

	return p.directory.PrincipalType(id)
}

// writeClientAuthError renders err in the shape of an Entra token endpoint failure.
func (h *Handler) writeClientAuthError(c *echo.Context, err *ClientAuthError) error {
	stamp := time.Now().UTC().Format("2006-01-02 15:04:05Z")
	trace, corr := uuid.NewString(), uuid.NewString()
	desc := fmt.Sprintf("%s\r\nTrace ID: %s\r\nCorrelation ID: %s\r\nTimestamp: %s", err.Description, trace, corr, stamp)

	return h.writeJSON(c, err.Status, map[string]any{
		"error": err.Name, "error_description": desc, "error_codes": []int{err.Code}, "timestamp": stamp,
		"trace_id": trace, "correlation_id": corr,
		"error_uri": "https://login.microsoftonline.com/error?code=" + fmt.Sprint(err.Code),
	})
}

// authenticateClient checks the request's client credentials. objectID is the token's oid claim, empty when the
// client is not in the directory.
func (h *Handler) authenticateClient(r *http.Request, clientID string, form map[string][]string) (string, *ClientAuthError) {
	if h.clients == nil || clientID == "" {
		return "", nil
	}

	secret := ""
	if v := form["client_secret"]; len(v) > 0 {
		secret = v[0]
	} else if _, pass, ok := r.BasicAuth(); ok {
		secret = pass
	}

	oid, known, err := h.clients.AuthenticateClient(clientID, secret)
	if err == nil || !known {
		return oid, nil
	}

	if ce, ok := err.(*ClientAuthError); ok { //nolint:errorlint // adapter returns the concrete type
		return "", ce
	}

	return "", &ClientAuthError{Status: http.StatusUnauthorized, Name: "invalid_client", Code: 7000215, Description: strings.TrimSpace(err.Error())}
}
