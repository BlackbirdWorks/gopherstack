package ociapigateway

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	keysRemoteDiscovery = "REMOTE_DISCOVERY"
	sourceDiscoveryURI  = "DISCOVERY_URI"
	clientDetailsCustom = "CUSTOM"
	introspectionLimit  = 10 * time.Second
	discoveryMaxBytes   = 1 << 20
)

// SecretResolver reads the current content of a secret (OCI Vault) by OCID and version number.
type SecretResolver func(id string, version int64) (string, bool)

type discoveryEntry struct {
	expires  time.Time
	endpoint string
}

// discoveryCache holds OpenID discovery results until maxCacheDurationInHours elapses on the backend clock.
type discoveryCache struct {
	mu      *lockmetrics.RWMutex
	entries map[string]discoveryEntry
}

func newDiscoveryCache() *discoveryCache {
	return &discoveryCache{mu: lockmetrics.New("ociapigateway.discovery"), entries: map[string]discoveryEntry{}}
}

func (c *discoveryCache) reset() {
	c.mu.Lock("reset")
	c.entries = map[string]discoveryEntry{}
	c.mu.Unlock()
}

func (c *discoveryCache) get(uri string, now time.Time) (string, bool) {
	c.mu.RLock("get")
	defer c.mu.RUnlock()

	e, ok := c.entries[uri]

	return e.endpoint, ok && e.expires.After(now)
}

func (c *discoveryCache) put(uri, endpoint string, expires time.Time) {
	c.mu.Lock("put")
	c.entries[uri] = discoveryEntry{endpoint: endpoint, expires: expires}
	c.mu.Unlock()
}

// SetSecretResolver wires OCI Vault for the client secret of REMOTE_DISCOVERY token validation.
func (b *InMemoryBackend) SetSecretResolver(fn SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	b.secrets = fn
	b.mu.Unlock()
}

type clientDetailsDoc struct {
	ClientSecretVersion *int64 `json:"clientSecretVersionNumber"`
	Type                string `json:"type"`
	ClientID            string `json:"clientId"`
	ClientSecretID      string `json:"clientSecretId"`
}

type sourceURIDoc struct {
	Type string `json:"type"`
	URI  string `json:"uri"`
}

func validateRemoteDiscovery(v *validationDoc) *APIError {
	if v.ClientDetails == nil || v.ClientDetails.Type != clientDetailsCustom {
		return errUnsupported("REMOTE_DISCOVERY clientDetails other than type CUSTOM")
	}

	cd := v.ClientDetails
	if cd.ClientID == "" || cd.ClientSecretID == "" || cd.ClientSecretVersion == nil {
		return errParam("clientDetails needs clientId, clientSecretId and clientSecretVersionNumber")
	}

	if v.SourceURI == nil || v.SourceURI.Type != sourceDiscoveryURI {
		return errUnsupported("REMOTE_DISCOVERY sourceUriDetails other than type DISCOVERY_URI")
	}

	if u, err := url.Parse(v.SourceURI.URI); err != nil || !httpURL(u) {
		return errParam("sourceUriDetails.uri must be an http or https URL")
	}

	return nil
}

func (b *InMemoryBackend) introspectionEndpoint(ctx context.Context, uri string, hours *int) (string, error) {
	now := b.clock()
	if ep, ok := b.disco.get(uri, now); ok {
		return ep, nil
	}

	ctx, cancel := context.WithTimeout(ctx, introspectionLimit)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return "", err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: %d", errJWKSStatus, resp.StatusCode)
	}

	var doc struct {
		Introspection string `json:"introspection_endpoint"`
	}

	if err = json.UnmarshalDecode(
		jsontext.NewDecoder(io.LimitReader(resp.Body, discoveryMaxBytes)),
		&doc,
		jsonx.V1(),
	); err != nil {
		return "", err
	}

	if u, perr := url.Parse(doc.Introspection); perr != nil || !httpURL(u) {
		return "", errKeyFormat
	}

	h := defaultCacheHr
	if hours != nil && *hours > 0 {
		h = *hours
	}

	b.disco.put(uri, doc.Introspection, now.Add(time.Duration(h)*time.Hour))

	return doc.Introspection, nil
}

// introspect validates an opaque token at the authorization server's RFC 7662 introspection endpoint, which is
// found through the OpenID discovery document.
func (b *InMemoryBackend) introspect(ctx context.Context, a *authDoc, token string) authResult {
	v := a.ValidationPolicy

	endpoint, err := b.introspectionEndpoint(ctx, v.SourceURI.URI, v.MaxCacheHours)
	if err != nil {
		return authResult{}
	}

	b.mu.RLock("introspect")
	resolve := b.secrets
	b.mu.RUnlock()

	if resolve == nil {
		return authResult{}
	}

	secret, ok := resolve(v.ClientDetails.ClientSecretID, *v.ClientDetails.ClientSecretVersion)
	if !ok {
		return authResult{}
	}

	ctx, cancel := context.WithTimeout(ctx, introspectionLimit)
	defer cancel()

	form := url.Values{"token": {token}, "token_type_hint": {"access_token"}}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return authResult{}
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(
		"Authorization",
		"Basic "+base64.StdEncoding.EncodeToString([]byte(v.ClientDetails.ClientID+":"+secret)),
	)

	resp, err := b.client.Do(req)
	if err != nil {
		return authResult{}
	}

	defer resp.Body.Close()

	return b.introspectionVerdict(a, resp)
}

func (b *InMemoryBackend) introspectionVerdict(a *authDoc, resp *http.Response) authResult {
	if resp.StatusCode != http.StatusOK {
		return authResult{}
	}

	claims := map[string]any{}
	if json.UnmarshalDecode(
		jsontext.NewDecoder(io.LimitReader(resp.Body, discoveryMaxBytes)),
		&claims,
		jsonx.V1(),
	) != nil {
		return authResult{}
	}

	if active, _ := claims["active"].(bool); !active {
		return authResult{}
	}

	now := b.clock()
	if exp, ok := claims["exp"].(float64); ok && !time.Unix(int64(exp), 0).After(now) {
		return authResult{}
	}

	if checkClaims(claims, effectiveChecks(a)) != nil {
		return authResult{}
	}

	return authResult{ok: true, scopes: scopesOf(claims), claims: claims}
}

// matchesKey reports whether a selection key matches the selector value.
func matchesKey(k *selectionKey, val string) bool {
	switch k.Type {
	case anyOf:
		return containsStr(k.Values, val)
	case wildcardKey:
		return wildcardMatch(k.Expression, val)
	default:
		return false
	}
}

// selectAuthServer picks the authentication server for dynamic authentication: exact keys, wildcard keys, default.
func selectAuthServer(d *dynamicAuthDoc, rv *reqView) *authDoc {
	val := rv.lookup(d.SelectionSource.Selector)

	for _, want := range []string{anyOf, wildcardKey} {
		for i := range d.Servers {
			if d.Servers[i].Key.Type == want && matchesKey(&d.Servers[i].Key, val) {
				return d.Servers[i].Detail
			}
		}
	}

	for i := range d.Servers {
		if def := d.Servers[i].Key.IsDefault; def != nil && *def {
			return d.Servers[i].Detail
		}
	}

	return nil
}

// authPolicy resolves the authentication policy in force; ok is false when dynamic selection finds no server.
func authPolicy(rp *requestPolicies, rv *reqView) (*authDoc, bool) {
	if rp == nil {
		return nil, true
	}

	if d := rp.DynamicAuthentication; d != nil {
		a := selectAuthServer(d, rv)

		return a, a != nil
	}

	return rp.Authentication, true
}
