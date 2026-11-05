package ociapigateway_test

import (
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jwtServer(keysJSON, issuer string) string {
	return `{"type":"JWT_AUTHENTICATION","issuers":["` + issuer + `"],"audiences":["` + jwtAudience + `"],` +
		`"tokenHeader":"Authorization","tokenAuthScheme":"Bearer","publicKeys":{"type":"STATIC_KEYS","keys":[` + keysJSON + `]}}`
}

func TestDynamicAuthentication(t *testing.T) {
	t.Parallel()

	tenantA, tenantB := rsaKey(t), rsaKey(t)
	spec := `{"requestPolicies":{"dynamicAuthentication":{"selectionSource":{"type":"SINGLE","selector":"request.headers[X-Tenant]"},` +
		`"authenticationServers":[` +
		`{"key":{"type":"ANY_OF","name":"a","values":["tenant-a"]},"authenticationServerDetail":` + jwtServer(jwkOf(tenantA, "ka"), "https://a.test") + `},` +
		`{"key":{"type":"WILDCARD","name":"b","expression":"tenant-b*"},"authenticationServerDetail":` + jwtServer(jwkOf(tenantB, "kb"), "https://b.test") + `},` +
		`{"key":{"type":"ANY_OF","name":"d","isDefault":true,"values":[]},"authenticationServerDetail":` + jwtServer(jwkOf(tenantA, "ka"), "https://a.test") + `}]}},` +
		`"routes":[` + stock("/x", `"GET"`, "in") + `]}`

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "dyn")
	e.mkDeployment(t, *gw.Id, "/d", spec)
	now := e.clock.Now()

	tok := func(k *rsa.PrivateKey, kid, iss string) string {
		c := goodClaims(now)
		c["iss"] = iss

		return "Bearer " + signToken(t, k, kid, c)
	}

	tests := []struct {
		name    string
		tenant  string
		auth    string
		want    int
		comment string
	}{
		{"exact key selects server a", "tenant-a", tok(tenantA, "ka", "https://a.test"), 200, ""},
		{"wildcard key selects server b", "tenant-b-eu", tok(tenantB, "kb", "https://b.test"), 200, ""},
		{"token for the other tenant is refused", "tenant-a", tok(tenantB, "kb", "https://b.test"), 401, ""},
		{"b server refuses a's issuer", "tenant-b", tok(tenantA, "ka", "https://a.test"), 401, ""},
		{"default key catches unknown selector values", "other", tok(tenantA, "ka", "https://a.test"), 200, ""},
		{"no token", "tenant-a", "", 401, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := map[string]string{"X-Tenant": tt.tenant}
			if tt.auth != "" {
				h["Authorization"] = tt.auth
			}

			assert.Equal(t, tt.want, e.call(t, *gw.Id, http.MethodGet, "/d/x", h, "").status)
		})
	}
}

func TestDynamicAuthenticationWithoutDefault(t *testing.T) {
	t.Parallel()

	k := rsaKey(t)
	spec := `{"requestPolicies":{"dynamicAuthentication":{"selectionSource":{"type":"SINGLE","selector":"request.query[t]"},` +
		`"authenticationServers":[{"key":{"type":"ANY_OF","name":"a","values":["a"]},"authenticationServerDetail":` +
		jwtServer(jwkOf(k, "k"), jwtIssuer) + `}]}},"routes":[` + stock("/x", `"GET"`, "in") + `]}`

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "dyn-nodefault")
	e.mkDeployment(t, *gw.Id, "/d", spec)

	h := map[string]string{"Authorization": "Bearer " + signToken(t, k, "k", goodClaims(e.clock.Now()))}

	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/d/x?t=a", h, "").status)
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/d/x?t=zzz", h, "").status, "no server selected fails closed")
}

type introspectionServer struct {
	*httptest.Server
	discoveries atomic.Int32
	calls       atomic.Int32
	lastAuth    atomic.Value
}

func newIntrospectionServer(t *testing.T, active func(token string) map[string]any) *introspectionServer {
	t.Helper()

	s := &introspectionServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		s.discoveries.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"introspection_endpoint": s.URL + "/introspect"})
	})
	mux.HandleFunc("/introspect", func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		s.lastAuth.Store(r.Header.Get("Authorization"))
		_ = r.ParseForm()

		out := active(r.PostForm.Get("token"))
		if out == nil {
			out = map[string]any{"active": false}
		}

		_ = json.NewEncoder(w).Encode(out)
	})

	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)

	return s
}

func TestTokenIntrospectionRemoteDiscovery(t *testing.T) {
	t.Parallel()

	exp := time.Now().Add(10 * time.Hour).Unix()
	idp := newIntrospectionServer(t, func(token string) map[string]any {
		switch token {
		case "good":
			return map[string]any{"active": true, "scope": "orders.read", "iss": jwtIssuer, "aud": jwtAudience, "exp": exp}
		case "wrong-issuer":
			return map[string]any{"active": true, "iss": "https://evil.test", "aud": jwtAudience, "exp": exp}
		case "expired":
			return map[string]any{"active": true, "iss": jwtIssuer, "aud": jwtAudience, "exp": 1}
		default:
			return nil
		}
	})

	e := newEnv(t, envOpts{})
	e.backend.SetSecretResolver(func(id string, version int64) (string, bool) {
		return "s3cret", id == "ocid1.vaultsecret.oc1..client" && version == 2
	})

	gw := e.mkGateway(t, "introspect")
	spec := `{"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION","tokenHeader":"Authorization",` +
		`"tokenAuthScheme":"Bearer","validationPolicy":{"type":"REMOTE_DISCOVERY","maxCacheDurationInHours":1,` +
		`"clientDetails":{"type":"CUSTOM","clientId":"cid","clientSecretId":"ocid1.vaultsecret.oc1..client","clientSecretVersionNumber":2},` +
		`"sourceUriDetails":{"type":"DISCOVERY_URI","uri":"` + idp.URL + `/.well-known/openid-configuration"},` +
		`"additionalValidationPolicy":{"issuers":["` + jwtIssuer + `"],"audiences":["` + jwtAudience + `"]}}}},` +
		`"routes":[` + stock("/x", `"GET"`, "in") +
		`,{"path":"/admin","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200},` +
		`"requestPolicies":{"authorization":{"type":"ANY_OF","allowedScope":["admin"]}}}]}`
	e.mkDeployment(t, *gw.Id, "/i", spec)

	bearer := func(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("good"), "").status)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("good"), "").status)
	assert.Equal(t, int32(1), idp.discoveries.Load(), "the discovery document is cached")
	assert.Equal(t, int32(2), idp.calls.Load(), "tokens are introspected on every request")
	assert.Equal(t, "Basic Y2lkOnMzY3JldA==", idp.lastAuth.Load(), "client credentials authenticate the introspection call")

	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("inactive"), "").status)
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("wrong-issuer"), "").status)
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("expired"), "").status)
	assert.Equal(t, 403, e.call(t, *gw.Id, http.MethodGet, "/i/admin", bearer("good"), "").status, "scopes come from the response")

	e.clock.Advance(2 * time.Hour)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/i/x", bearer("good"), "").status)
	assert.Equal(t, int32(2), idp.discoveries.Load(), "discovery cache expires on the clock")
}

func TestTokenIntrospectionFailsClosed(t *testing.T) {
	t.Parallel()

	idp := newIntrospectionServer(t, func(string) map[string]any {
		return map[string]any{"active": true, "exp": time.Now().Add(time.Hour).Unix()}
	})

	u, err := url.Parse(idp.URL)
	require.NoError(t, err)

	spec := func(secretID string) string {
		return `{"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION","tokenHeader":"Authorization",` +
			`"tokenAuthScheme":"Bearer","validationPolicy":{"type":"REMOTE_DISCOVERY",` +
			`"clientDetails":{"type":"CUSTOM","clientId":"c","clientSecretId":"` + secretID + `","clientSecretVersionNumber":1},` +
			`"sourceUriDetails":{"type":"DISCOVERY_URI","uri":"http://` + u.Host + `/.well-known/openid-configuration"}}}},` +
			`"routes":[` + stock("/x", `"GET"`, "in") + `]}`
	}

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "introspect-closed")
	e.mkDeployment(t, *gw.Id, "/nores", spec("ocid1.vaultsecret.oc1..a"))

	h := map[string]string{"Authorization": "Bearer t"}
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/nores/x", h, "").status, "no secret resolver wired")

	e.backend.SetSecretResolver(func(string, int64) (string, bool) { return "", false })
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/nores/x", h, "").status, "unknown secret")

	_, err = e.createDeployment(t, *gw.Id, "/bad", specFromJSON(t, `{"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION",`+
		`"validationPolicy":{"type":"REMOTE_DISCOVERY","clientDetails":{"type":"VALIDATION_BLOCK"}}}},"routes":[]}`))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	var _ jwt.Claims = jwt.MapClaims{}
}
