package azurearm_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json/v2"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const onErrReason = `<on-error><set-header name="X-Reason" exists-action="override">` +
	`<value>@(context.LastError.Reason)</value></set-header></on-error>`

func jwtPolicy(inbound string) string {
	return "<policies><inbound><base />" + inbound + `</inbound><backend><base /></backend><outbound><base /></outbound>` +
		onErrReason + "</policies>"
}

func signingKey(k string) string {
	return `<issuer-signing-keys><key>` + k + `</key></issuer-signing-keys>`
}

func signHS(t *testing.T, alg jwt.SigningMethod, secret []byte, claims jwt.MapClaims) string {
	t.Helper()

	s, err := jwt.NewWithClaims(alg, claims).SignedString(secret)
	require.NoError(t, err)

	return s
}

func TestAPIMGateway_ValidateJWTSymmetric(t *testing.T) {
	t.Parallel()

	secret := []byte("0123456789abcdef0123456789abcdef")
	e := newGWEnv(t)
	now := e.clock.Now()

	e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="Authorization" require-scheme="Bearer"
		output-token-variable-name="jwt" clock-skew="30">
		<issuer-signing-keys><key>`+base64.StdEncoding.EncodeToString(secret)+`</key></issuer-signing-keys>
		<audiences><audience>api://x</audience></audiences><issuers><issuer>iss1</issuer></issuers>
		<required-claims><claim name="role" match="any"><value>admin</value><value>ops</value></claim></required-claims>
		</validate-jwt>`+headerPolicy("X-Sub", `@(((Jwt)context.Variables["jwt"]).Subject)`)), http.StatusCreated)

	good := func(over jwt.MapClaims) jwt.MapClaims {
		c := jwt.MapClaims{
			"sub": "s1", "aud": "api://x", "iss": "iss1", "role": []string{"ops"},
			"exp": now.Add(time.Hour).Unix(),
		}
		for k, v := range over {
			if v == nil {
				delete(c, k)

				continue
			}

			c[k] = v
		}

		return c
	}

	other := []byte("ffffffffffffffffffffffffffffffff")
	noneTok, err := jwt.NewWithClaims(jwt.SigningMethodNone, good(nil)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name       string
		auth       string
		wantStatus int
		wantReason string
	}{
		{"valid hs256", "Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(nil)), 200, ""},
		{"valid hs384", "Bearer " + signHS(t, jwt.SigningMethodHS384, secret, good(nil)), 200, ""},
		{"valid hs512", "Bearer " + signHS(t, jwt.SigningMethodHS512, secret, good(nil)), 200, ""},
		{
			"within clock skew",
			"Bearer " + signHS(
				t,
				jwt.SigningMethodHS256,
				secret,
				good(jwt.MapClaims{"exp": now.Add(-10 * time.Second).Unix()}),
			),
			200,
			"",
		},
		{
			"expired",
			"Bearer " + signHS(
				t,
				jwt.SigningMethodHS256,
				secret,
				good(jwt.MapClaims{"exp": now.Add(-time.Minute).Unix()}),
			),
			401,
			"TokenExpired",
		},
		{
			"no exp",
			"Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(jwt.MapClaims{"exp": nil})),
			401,
			"JwtInvalid",
		},
		{"wrong key", "Bearer " + signHS(t, jwt.SigningMethodHS256, other, good(nil)), 401, "TokenSignatureInvalid"},
		{"alg none", "Bearer " + noneTok, 401, "TokenSignatureInvalid"},
		{
			"wrong audience",
			"Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(jwt.MapClaims{"aud": "other"})),
			401,
			"TokenAudienceNotAllowed",
		},
		{
			"wrong issuer",
			"Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(jwt.MapClaims{"iss": "evil"})),
			401,
			"TokenIssuerNotAllowed",
		},
		{
			"missing claim",
			"Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(jwt.MapClaims{"role": nil})),
			401,
			"TokenClaimNotFound",
		},
		{
			"claim value",
			"Bearer " + signHS(t, jwt.SigningMethodHS256, secret, good(jwt.MapClaims{"role": []string{"guest"}})),
			401,
			"TokenClaimValueNotAllowed",
		},
		{"no token", "", 401, "TokenNotPresent"},
		{"wrong scheme", "Basic " + signHS(t, jwt.SigningMethodHS256, secret, good(nil)), 401, "TokenNotPresent"},
		{"garbage", "Bearer not.a.jwt", 401, "JwtInvalid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, body, hdr := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Authorization": tc.auth})
			require.Equal(t, tc.wantStatus, status, body)

			if tc.wantStatus == 200 {
				assert.Equal(t, []string{"s1"}, decodeEcho(t, body).Header["X-Sub"])

				return
			}

			assert.Equal(t, tc.wantReason, hdr.Get("X-Reason"))
		})
	}
}

func TestAPIMGateway_ValidateJWTSources(t *testing.T) {
	t.Parallel()

	secret := []byte("0123456789abcdef0123456789abcdef")
	k := base64.StdEncoding.EncodeToString(secret)
	e := newGWEnv(t)
	tok := signHS(t, jwt.SigningMethodHS256, secret, jwt.MapClaims{"exp": e.clock.Now().Add(time.Hour).Unix()})

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name       string
		policy     string
		path       string
		hdr        map[string]string
		wantStatus int
		wantBody   string
	}{
		{
			"query parameter",
			`<validate-jwt query-parameter-name="tok">` + signingKey(k) + `</validate-jwt>`,
			"/echo/items/1?tok=" + tok,
			nil,
			200,
			"",
		},
		{
			"token-value expression",
			`<validate-jwt token-value="@(context.Request.Headers.GetValueOrDefault(&quot;X-T&quot;, &quot;&quot;))">` +
				signingKey(k) + `</validate-jwt>`,
			"/echo/items/1", map[string]string{"X-T": tok}, 200, "",
		},
		{
			"custom failure",
			`<validate-jwt header-name="X-T" failed-validation-httpcode="403" ` +
				`failed-validation-error-message="denied here">` + signingKey(k) + `</validate-jwt>`,
			"/echo/items/1", map[string]string{"X-T": "x.y.z"}, 403, "denied here",
		},
		{
			"unsigned allowed when explicitly not required",
			`<validate-jwt header-name="X-T" require-signed-tokens="false" require-expiration-time="false" />`,
			"/echo/items/1", map[string]string{"X-T": unsignedToken(t)}, 200, "",
		},
		{
			"unsigned refused by default",
			`<validate-jwt header-name="X-T" require-expiration-time="false">` + signingKey(k) + `</validate-jwt>`,
			"/echo/items/1", map[string]string{"X-T": unsignedToken(t)}, 401, "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newGWEnv(t)
			env.policy(t, "/apis/echo", jwtPolicy(tc.policy), http.StatusCreated)

			status, body, _ := env.call(t, "GET", tc.path, "", tc.hdr)
			assert.Equal(t, tc.wantStatus, status, body)
			assert.Contains(t, body, tc.wantBody)
		})
	}
}

func unsignedToken(t *testing.T) string {
	t.Helper()

	s, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"sub": "x"}).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	return s
}

func b64(b *big.Int) string { return base64.RawURLEncoding.EncodeToString(b.Bytes()) }

func TestAPIMGateway_ValidateJWTAlgorithmConfusion(t *testing.T) {
	t.Parallel()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	e := newGWEnv(t)
	exp := e.clock.Now().Add(time.Hour).Unix()

	e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="X-T"><issuer-signing-keys><key n="`+b64(priv.N)+
		`" e="`+b64(big.NewInt(int64(priv.E)))+`" /></issuer-signing-keys></validate-jwt>`), http.StatusCreated)

	rs := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": exp})
	good, err := rs.SignedString(priv)
	require.NoError(t, err)

	confused := signHS(t, jwt.SigningMethodHS256, priv.N.Bytes(), jwt.MapClaims{"exp": exp})

	for name, tc := range map[string]struct {
		tok  string
		want int
	}{"rs256": {good, 200}, "hs256 with the public modulus as secret": {confused, 401}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			status, body, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": tc.tok})
			assert.Equal(t, tc.want, status, body)
		})
	}
}

type jwksServer struct {
	srv       *httptest.Server
	keys      map[string]*rsa.PublicKey
	issuer    string
	mu        sync.Mutex
	jwksFetch atomic.Int64
}

func newJWKSServer(t *testing.T, issuer string) *jwksServer {
	t.Helper()

	js := &jwksServer{keys: map[string]*rsa.PublicKey{}, issuer: issuer}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.MarshalWrite(w, map[string]string{"issuer": issuer, "jwks_uri": js.srv.URL + "/jwks"}, jsonx.V1())
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		js.jwksFetch.Add(1)
		js.mu.Lock()
		defer js.mu.Unlock()

		keys := make([]map[string]string, 0, len(js.keys))
		for kid, pk := range js.keys {
			keys = append(
				keys,
				map[string]string{"kty": "RSA", "kid": kid, "n": b64(pk.N), "e": b64(big.NewInt(int64(pk.E)))},
			)
		}

		_ = json.MarshalWrite(w, map[string]any{"keys": keys}, jsonx.V1())
	})
	js.srv = httptest.NewServer(mux)
	t.Cleanup(js.srv.Close)

	return js
}

func (j *jwksServer) add(kid string, pk *rsa.PublicKey) {
	j.mu.Lock()
	j.keys[kid] = pk
	j.mu.Unlock()
}

func TestAPIMGateway_ValidateJWTOpenIDConfig(t *testing.T) {
	t.Parallel()

	k1, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	k2, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	js := newJWKSServer(t, "https://issuer.example/")
	js.add("k1", &k1.PublicKey)

	e := newGWEnv(t)
	exp := e.clock.Now().Add(time.Hour).Unix()

	e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="X-T"><openid-config url="`+js.srv.URL+
		`/.well-known/openid-configuration" /></validate-jwt>`), http.StatusCreated)

	sign := func(kid string, key *rsa.PrivateKey, iss string) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": exp, "iss": iss})
		tok.Header["kid"] = kid

		s, serr := tok.SignedString(key)
		require.NoError(t, serr)

		return s
	}

	call := func(tok string) int {
		status, _, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": tok})

		return status
	}

	assert.Equal(t, 200, call(sign("k1", k1, "https://issuer.example/")))
	assert.Equal(t, 200, call(sign("k1", k1, "https://issuer.example/")))
	assert.Equal(t, int64(1), js.jwksFetch.Load(), "the JWKS is cached")

	assert.Equal(t, 401, call(sign("k1", k1, "https://evil/")), "the issuer comes from the configuration")

	js.add("k2", &k2.PublicKey)
	assert.Equal(
		t,
		401,
		call(sign("k2", k2, "https://issuer.example/")),
		"an unknown kid does not refetch within 5 minutes",
	)
	assert.Equal(t, int64(1), js.jwksFetch.Load())

	e.clock.Advance(6 * time.Minute)

	exp = e.clock.Now().Add(time.Hour).Unix()
	assert.Equal(t, 200, call(sign("k2", k2, "https://issuer.example/")), "the refresh picks up the rotated key")
	assert.Equal(t, int64(2), js.jwksFetch.Load())
}

func TestAPIMJWT_Validation(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	key := `<issuer-signing-keys><key>` + base64.StdEncoding.EncodeToString(
		[]byte("0123456789abcdef"),
	) + `</key></issuer-signing-keys>`

	tests := []struct{ name, stmt, want string }{
		{"no source", `<validate-jwt>` + key + `</validate-jwt>`, "Exactly one of"},
		{
			"two sources",
			`<validate-jwt header-name="a" query-parameter-name="b">` + key + `</validate-jwt>`,
			"Exactly one of",
		},
		{"no keys", `<validate-jwt header-name="a" />`, "openid-config or issuer-signing-keys"},
		{
			"bad http code",
			`<validate-jwt header-name="a" failed-validation-httpcode="99">` + key + `</validate-jwt>`,
			"'failed-validation-httpcode'",
		},
		{
			"bad boolean",
			`<validate-jwt header-name="a" require-signed-tokens="maybe">` + key + `</validate-jwt>`,
			"require-signed-tokens",
		},
		{
			"decryption keys",
			`<validate-jwt header-name="a">` + key + `<decryption-keys><key>QQ==</key></decryption-keys></validate-jwt>`,
			"not supported inside",
		},
		{
			"bad base64",
			`<validate-jwt header-name="a"><issuer-signing-keys><key>!!!</key></issuer-signing-keys></validate-jwt>`,
			"not valid Base64",
		},
		{
			"n without e",
			`<validate-jwt header-name="a"><issuer-signing-keys><key n="AQAB" /></issuer-signing-keys></validate-jwt>`,
			"given together",
		},
		{
			"bad match",
			`<validate-jwt header-name="a">` + key +
				`<required-claims><claim name="c" match="some" /></required-claims></validate-jwt>`,
			"'match'",
		},
		{
			"bad openid url",
			`<validate-jwt header-name="a"><openid-config url="ftp://x" /></validate-jwt>`,
			"absolute http or https",
		},
		{
			"certificate-id",
			`<validate-jwt header-name="a"><issuer-signing-keys><key certificate-id="c" /></issuer-signing-keys></validate-jwt>`,
			"does not exist",
		},
		{"in outbound", "", "not allowed in the outbound"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc := policyDoc(tc.stmt, "", "")
			if tc.stmt == "" {
				doc = policyDoc("", "", `<validate-jwt header-name="a">`+key+`</validate-jwt>`)
			}

			body := e.policy(t, "/apis/echo", doc, http.StatusBadRequest)
			assert.Contains(t, body, tc.want)
		})
	}
}
