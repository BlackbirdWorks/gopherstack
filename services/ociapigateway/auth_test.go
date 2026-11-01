package ociapigateway_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

const (
	jwtIssuer   = "https://issuer.example.test"
	jwtAudience = "api://orders"
)

func rsaKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	return k
}

func jwkOf(k *rsa.PrivateKey, kid string) string {
	n := base64.RawURLEncoding.EncodeToString(k.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(k.E)).Bytes())

	return `{"format":"JSON_WEB_KEY","kid":"` + kid + `","kty":"RSA","alg":"RS256","use":"sig",` +
		`"n":"` + n + `","e":"` + e + `"}`
}

func pemOf(t *testing.T, k *rsa.PrivateKey, kid string) string {
	t.Helper()

	der, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)

	b, err := json.Marshal(map[string]string{
		"format": "PEM", "kid": kid, "key": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
	})
	require.NoError(t, err)

	return string(b)
}

func signToken(t *testing.T, k *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid

	s, err := tok.SignedString(k)
	require.NoError(t, err)

	return s
}

func goodClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": jwtIssuer, "aud": jwtAudience, "sub": "u1", "scope": "orders.read orders.write",
		"exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "tier": "gold",
	}
}

func authSpec(keysJSON, extra string) string {
	return `{"requestPolicies":{"authentication":{"type":"JWT_AUTHENTICATION","issuers":["` + jwtIssuer +
		`"],"audiences":["` + jwtAudience + `"],"tokenHeader":"Authorization","tokenAuthScheme":"Bearer",` + extra +
		`"publicKeys":` + keysJSON + `}},"routes":[` + stock("/secure", `"GET"`, "secret") + `,
		{"path":"/scoped","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"scoped"},
		 "requestPolicies":{"authorization":{"type":"ANY_OF","allowedScope":["admin","orders.write"]}}},
		{"path":"/needs-admin","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200},
		 "requestPolicies":{"authorization":{"type":"ANY_OF","allowedScope":["admin"]}}},
		{"path":"/public","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"public"},
		 "requestPolicies":{"authorization":{"type":"ANONYMOUS"}}}]}`
}

func TestJWTStaticKeys(t *testing.T) {
	t.Parallel()

	good, other := rsaKey(t), rsaKey(t)
	keyVariants := map[string]string{
		"jwk": `{"type":"STATIC_KEYS","keys":[` + jwkOf(good, "k1") + `]}`,
		"pem": `{"type":"STATIC_KEYS","keys":[` + pemOf(t, good, "k1") + `]}`,
	}

	for variant, keys := range keyVariants {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			gw := e.mkGateway(t, "jwt")
			e.mkDeployment(
				t,
				*gw.Id,
				"/j",
				authSpec(keys, `"isAnonymousAccessAllowed":true,"maxClockSkewInSeconds":30,`),
			)
			now := e.clock.Now()

			bad := func(mod func(jwt.MapClaims)) string {
				c := goodClaims(now)
				mod(c)

				return signToken(t, good, "k1", c)
			}

			hs, err := jwt.NewWithClaims(jwt.SigningMethodHS256, goodClaims(now)).SignedString([]byte("secret"))
			require.NoError(t, err)

			none, err := jwt.NewWithClaims(jwt.SigningMethodNone, goodClaims(now)).
				SignedString(jwt.UnsafeAllowNoneSignatureType)
			require.NoError(t, err)

			tests := []struct {
				name, path, token string
				status            int
			}{
				{"valid", "/j/secure", signToken(t, good, "k1", goodClaims(now)), 200},
				{"valid no kid match single key", "/j/secure", signToken(t, good, "", goodClaims(now)), 200},
				{"wrong issuer", "/j/secure", bad(func(c jwt.MapClaims) { c["iss"] = "https://evil" }), 401},
				{"wrong audience", "/j/secure", bad(func(c jwt.MapClaims) { c["aud"] = "other" }), 401},
				{
					"audience list",
					"/j/secure",
					bad(func(c jwt.MapClaims) { c["aud"] = []string{"x", jwtAudience} }),
					200,
				},
				{"expired", "/j/secure", bad(func(c jwt.MapClaims) { c["exp"] = now.Add(-time.Hour).Unix() }), 401},
				{
					"expired within skew",
					"/j/secure",
					bad(func(c jwt.MapClaims) { c["exp"] = now.Add(-10 * time.Second).Unix() }),
					200,
				},
				{
					"not yet valid",
					"/j/secure",
					bad(func(c jwt.MapClaims) { c["nbf"] = now.Add(time.Hour).Unix() }),
					401,
				},
				{"missing exp", "/j/secure", bad(func(c jwt.MapClaims) { delete(c, "exp") }), 401},
				{"wrong signing key", "/j/secure", signToken(t, other, "k1", goodClaims(now)), 401},
				{"unknown kid", "/j/secure", signToken(t, good, "zzz", goodClaims(now)), 401},
				{"hmac confusion", "/j/secure", hs, 401},
				{"alg none", "/j/secure", none, 401},
				{"garbage", "/j/secure", "not.a.jwt", 401},
				{"no token anonymous allowed", "/j/secure", "", 200},
				{"scope any_of match", "/j/scoped", signToken(t, good, "k1", goodClaims(now)), 200},
				{"scope any_of miss", "/j/needs-admin", signToken(t, good, "k1", goodClaims(now)), 403},
				{"anonymous route with bad token", "/j/public", "garbage", 200},
				{"anonymous route no token", "/j/public", "", 200},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					hdr := map[string]string{}
					if tt.token != "" {
						hdr["Authorization"] = "Bearer " + tt.token
					}

					assert.Equal(t, tt.status, e.call(t, *gw.Id, http.MethodGet, tt.path, hdr, "").status)
				})
			}
		})
	}
}

func TestJWTRequiredWhenAnonymousDisallowed(t *testing.T) {
	t.Parallel()

	k := rsaKey(t)
	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "jwt2")
	e.mkDeployment(
		t,
		*gw.Id,
		"/j",
		`{"requestPolicies":{"authentication":{"type":"JWT_AUTHENTICATION","issuers":["`+jwtIssuer+
			`"],"audiences":["`+jwtAudience+`"],"tokenQueryParam":"access_token",`+
			`"publicKeys":{"type":"STATIC_KEYS","keys":[`+jwkOf(k, "k1")+
			`]}}},"routes":[`+stock("/x", `"GET"`, "ok")+`]}`,
	)

	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/j/x", nil, "").status)

	tok := signToken(t, k, "k1", goodClaims(e.clock.Now()))
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/j/x?access_token="+tok, nil, "").status)

	e.clock.Advance(2 * time.Hour)
	assert.Equal(
		t,
		401,
		e.call(t, *gw.Id, http.MethodGet, "/j/x?access_token="+tok, nil, "").status,
		"expiry follows the injected clock",
	)
}

func TestJWTVerifyClaims(t *testing.T) {
	t.Parallel()

	k := rsaKey(t)
	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "claims")
	e.mkDeployment(t, *gw.Id, "/c", authSpec(
		`{"type":"STATIC_KEYS","keys":[`+jwkOf(k, "k1")+`]}`,
		`"isAnonymousAccessAllowed":true,"verifyClaims":[{"key":"tier","values":["gold","silver"]},`+
			`{"key":"dept","isRequired":false,"values":["eng"]}],`,
	))
	now := e.clock.Now()

	tests := []struct {
		mod    func(jwt.MapClaims)
		name   string
		status int
	}{
		{func(jwt.MapClaims) {}, "claim matches", 200},
		{func(c jwt.MapClaims) { c["tier"] = "bronze" }, "required claim wrong value", 401},
		{func(c jwt.MapClaims) { delete(c, "tier") }, "required claim missing", 401},
		{func(c jwt.MapClaims) { c["dept"] = "ops" }, "optional claim wrong value", 401},
		{func(c jwt.MapClaims) { c["dept"] = "eng" }, "optional claim right value", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := goodClaims(now)
			tt.mod(c)

			r := e.call(t, *gw.Id, http.MethodGet, "/c/secure", map[string]string{
				"Authorization": "Bearer " + signToken(t, k, "k1", c),
			}, "")
			assert.Equal(t, tt.status, r.status)
		})
	}
}

func TestJWTRemoteJWKS(t *testing.T) {
	t.Parallel()

	k := rsaKey(t)

	var fetches atomic.Int32

	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)

		var key map[string]any
		_ = json.Unmarshal([]byte(jwkOf(k, "r1")), &key)

		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{key}})
	}))
	t.Cleanup(jwks.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "remote")
	e.mkDeployment(
		t,
		*gw.Id,
		"/r",
		authSpec(
			`{"type":"REMOTE_JWKS","uri":"`+jwks.URL+`","maxCacheDurationInHours":2}`,
			`"isAnonymousAccessAllowed":true,`,
		),
	)

	hdr := func() map[string]string {
		return map[string]string{"Authorization": "Bearer " + signToken(t, k, "r1", goodClaims(e.clock.Now()))}
	}

	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/r/secure", hdr(), "").status)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/r/secure", hdr(), "").status)
	assert.Equal(t, int32(1), fetches.Load(), "key set is cached")

	e.clock.Advance(3 * time.Hour)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/r/secure", hdr(), "").status)
	assert.Equal(t, int32(2), fetches.Load(), "cache expires after maxCacheDurationInHours")
}

func TestJWTRemoteJWKSBlockedOffLoopback(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "ssrf")
	e.mkDeployment(
		t,
		*gw.Id,
		"/r",
		authSpec(`{"type":"REMOTE_JWKS","uri":"http://192.0.2.7:9/jwks"}`, `"isAnonymousAccessAllowed":true,`),
	)

	r := e.call(t, *gw.Id, http.MethodGet, "/r/secure", map[string]string{"Authorization": "Bearer abc.def.ghi"}, "")
	assert.Equal(t, 401, r.status, "unreachable JWKS fails closed")
}

func TestTokenAuthenticationAdditionalPolicy(t *testing.T) {
	t.Parallel()

	k := rsaKey(t)
	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "tokenauth")
	e.mkDeployment(
		t,
		*gw.Id,
		"/t",
		`{"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION","tokenHeader":"X-Token",
		"tokenAuthScheme":"Bearer","validationPolicy":{"type":"STATIC_KEYS","keys":[`+jwkOf(k, "k1")+`],
		"additionalValidationPolicy":{"issuers":["`+jwtIssuer+`"],"audiences":["`+jwtAudience+`"]}}}},
		"routes":[`+stock("/x", `"GET"`, "ok")+`]}`,
	)

	now := e.clock.Now()
	tok := signToken(t, k, "k1", goodClaims(now))

	assert.Equal(
		t,
		200,
		e.call(t, *gw.Id, http.MethodGet, "/t/x", map[string]string{"X-Token": "Bearer " + tok}, "").status,
	)
	assert.Equal(
		t,
		401,
		e.call(t, *gw.Id, http.MethodGet, "/t/x", map[string]string{"Authorization": "Bearer " + tok}, "").status,
	)

	c := goodClaims(now)
	c["iss"] = "x"
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/t/x",
		map[string]string{"X-Token": "Bearer " + signToken(t, k, "k1", c)}, "").status)
}

func TestCustomAuthenticator(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	inv := &fakeInvoker{fn: func(_ string, in ociapigateway.FunctionRequest) (ociapigateway.FunctionResponse, error) {
		var p struct {
			Data  map[string]string `json:"data"`
			Type  string            `json:"type"`
			Token string            `json:"token"`
		}

		_ = json.Unmarshal(in.Body, &p)

		verdict := map[string]any{"active": false, "wwwAuthenticate": `Bearer realm="orders"`}

		switch {
		case p.Type == "TOKEN" && p.Token == "good":
			verdict = map[string]any{"active": true, "scope": []string{"orders.write"}}
		case p.Type == "USER_DEFINED" && p.Data["key"] == "letmein":
			verdict = map[string]any{"active": true, "scope": []string{"orders.write"}}
		}

		out, _ := json.Marshal(verdict)

		return ociapigateway.FunctionResponse{Status: 200, Body: out}, nil
	}}
	e.backend.SetFunctionInvoker(inv)

	gw := e.mkGateway(t, "custom")
	e.mkDeployment(
		t,
		*gw.Id,
		"/a",
		`{"requestPolicies":{"authentication":{"type":"CUSTOM_AUTHENTICATION","functionId":"`+testFunction+
			`","tokenHeader":"Authorization"}},"routes":[`+stock("/x", `"GET"`, "ok")+`,
		{"path":"/scoped","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200},
		 "requestPolicies":{"authorization":{"type":"ANY_OF","allowedScope":["admin"]}}}]}`,
	)

	assert.Equal(
		t,
		200,
		e.call(t, *gw.Id, http.MethodGet, "/a/x", map[string]string{"Authorization": "good"}, "").status,
	)

	denied := e.call(t, *gw.Id, http.MethodGet, "/a/x", map[string]string{"Authorization": "bad"}, "")
	assert.Equal(t, 401, denied.status)
	assert.Equal(t, `Bearer realm="orders"`, denied.header.Get("WWW-Authenticate"))

	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/a/x", nil, "").status)
	assert.Equal(
		t,
		403,
		e.call(t, *gw.Id, http.MethodGet, "/a/scoped", map[string]string{"Authorization": "good"}, "").status,
	)

	e.mkDeployment(
		t,
		*gw.Id,
		"/b",
		`{"requestPolicies":{"authentication":{"type":"CUSTOM_AUTHENTICATION","functionId":"`+testFunction+
			`","tokenHeader":"X-Auth","parameters":{"key":"request.headers[X-Key]"}}},`+
			`"routes":[`+stock("/x", `"GET"`, "ok")+`]}`,
	)

	assert.Equal(
		t,
		200,
		e.call(t, *gw.Id, http.MethodGet, "/b/x", map[string]string{"X-Auth": "t", "X-Key": "letmein"}, "").status,
	)
	assert.Equal(
		t,
		401,
		e.call(t, *gw.Id, http.MethodGet, "/b/x", map[string]string{"X-Auth": "t", "X-Key": "no"}, "").status,
	)
}
