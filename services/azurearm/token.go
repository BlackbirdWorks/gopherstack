package azurearm

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
)

// defaultTokenAudience is used when a client-credentials request's "scope"
// form field is empty or doesn't carry a resource prefix.
const defaultTokenAudience = "https://management.azure.com/" //nolint:gosec // URL, not a credential

// TokenResponse is the client-credentials grant response body, for both the
// v1 (/{tenant}/oauth2/token) and v2 (/{tenant}/oauth2/v2.0/token) endpoints
// -- AZURE.md section 10.1 specifies the identical body shape for both.
type TokenResponse struct {
	TokenType   string `json:"token_type"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// audienceFromScope extracts the resource audience from an OAuth2 "scope"
// (v2 shape, e.g. "https://management.azure.com/.default") or "resource"
// (v1 shape) form value, stripping a trailing "/.default" if present.
func audienceFromScope(scopeOrResource string) string {
	s := ""

	for f := range strings.FieldsSeq(scopeOrResource) {
		if isOIDCScope(f) {
			continue
		}

		s = f

		break
	}

	if s == "" {
		return defaultTokenAudience
	}

	s = strings.TrimSuffix(s, "/.default")
	if !strings.HasSuffix(s, "/") {
		s += "/"
	}

	return s
}

// IssueToken issues a client-credentials access token for tenant, using
// issuer, returning the wire response body.
func IssueToken(issuer *aadauth.Issuer, baseURL, tenant, clientID, scopeOrResource string) (TokenResponse, error) {
	return IssueTokenForObject(issuer, baseURL, tenant, clientID, "", scopeOrResource)
}

// IssueTokenForObject is IssueToken with an explicit oid/sub claim, the client's service principal object ID.
func IssueTokenForObject(
	issuer *aadauth.Issuer, baseURL, tenant, clientID, objectID, scopeOrResource string,
) (TokenResponse, error) {
	audience := audienceFromScope(scopeOrResource)

	claims := aadauth.ClientCredentialsClaims{
		Issuer:   baseURL + "/" + tenant + "/v2.0",
		Audience: audience,
		TenantID: tenant,
		ClientID: clientID,
		ObjectID: objectID,
	}

	tokenString, err := issuer.IssueClientCredentialsToken(claims, aadauth.DefaultTokenExpirySeconds)
	if err != nil {
		return TokenResponse{}, err
	}

	return TokenResponse{
		TokenType:   "Bearer",
		ExpiresIn:   aadauth.DefaultTokenExpirySeconds,
		AccessToken: tokenString,
	}, nil
}

// issueUserToken issues the delegated token of a password sign-in: the user's oid and upn, the tenant's own ID as
// tid whatever alias the URL used, and the scopes the request named (v2.0 scp).
func issueUserToken(
	issuer *aadauth.Issuer, baseURL, tenantID, clientID string, user signedInUser, scopeOrResource string,
) (TokenResponse, error) {
	audience, scopes := userAudienceAndScopes(scopeOrResource)

	tokenString, err := issuer.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
		Issuer: baseURL + "/" + tenantID + "/v2.0", Audience: audience, TenantID: tenantID,
		ClientID: clientID, ObjectID: user.oid, UserPrincipalName: user.upn, Scope: scopes,
	}, aadauth.DefaultTokenExpirySeconds)
	if err != nil {
		return TokenResponse{}, err
	}

	return TokenResponse{
		TokenType:   "Bearer",
		ExpiresIn:   aadauth.DefaultTokenExpirySeconds,
		AccessToken: tokenString,
	}, nil
}

const graphAudience = "https://graph.microsoft.com/"

// userAudienceAndScopes splits a delegated scope request: a bare scope name such as User.Read belongs to Microsoft
// Graph, a URL scope names its resource, and ".default" asks for no particular scope.
func userAudienceAndScopes(scopeOrResource string) (string, string) {
	audience := ""

	var names []string

	for f := range strings.FieldsSeq(scopeOrResource) {
		if isOIDCScope(f) {
			continue
		}

		resource, name := "", f

		if i := strings.LastIndex(f, "/"); i >= 0 {
			resource, name = f[:i+1], f[i+1:]
		}

		if audience == "" {
			audience = resource
		}

		if name != ".default" && name != "" {
			names = append(names, name)
		}
	}

	switch {
	case audience != "":
	case len(names) > 0:
		audience = graphAudience
	default:
		audience = defaultTokenAudience
	}

	return audience, strings.Join(names, " ")
}

func isOIDCScope(s string) bool {
	switch s {
	case "openid", "profile", "offline_access", "email":
		return true
	}

	return false
}
