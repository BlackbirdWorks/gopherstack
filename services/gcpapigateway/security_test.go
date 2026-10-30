package gcpapigateway_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	issuer  = "https://issuer.example.com"
	otherIs = "https://other.example.com"
)

func TestAPIKeySecurity(t *testing.T) {
	t.Parallel()

	const top = "securityDefinitions:\n  api_key:\n    type: apiKey\n    name: key\n    in: query\n"

	paths := "  /locked:\n" + op("get", "      security:\n        - api_key: []\n") + "  /open:\n" + op("get", "")

	tests := []struct {
		hdr    map[string]string
		name   string
		target string
		want   int
	}{
		{name: "missing key", target: "/locked", want: http.StatusUnauthorized},
		{name: "empty key", target: "/locked?key=", want: http.StatusUnauthorized},
		{name: "query key", target: "/locked?key=abc", want: http.StatusOK},
		{name: "header key", target: "/locked", hdr: map[string]string{"x-api-key": "abc"}, want: http.StatusOK},
		{name: "unsecured path", target: "/open", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := echoBackend(t)
			e := newEnv(t)
			gw := e.deploy(t, opSpec(be.URL, top, paths))

			got := e.call(t, gw, http.MethodGet, tt.target, tt.hdr)
			assert.Equal(t, tt.want, got.status, got.raw)

			if tt.want == http.StatusUnauthorized {
				assert.EqualValues(t, "UNAUTHENTICATED: Method doesn't allow unregistered callers (callers without "+
					"established identity). Please use API Key or other form of API consumer identity to call this API.",
					got.body["message"])
			}
		})
	}
}

func TestRootSecurityAndAnonymousOverride(t *testing.T) {
	t.Parallel()

	const top = "securityDefinitions:\n  api_key:\n    type: apiKey\n    name: key\n    in: query\n" +
		"security:\n  - api_key: []\n"

	paths := "  /locked:\n" + op("get", "") + "  /anon:\n" + op("get", "      security: []\n")

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, top, paths))

	assert.Equal(t, http.StatusUnauthorized, e.call(t, gw, http.MethodGet, "/locked", nil).status)
	assert.Equal(t, http.StatusOK, e.call(t, gw, http.MethodGet, "/anon", nil).status)
}

type signer struct {
	rsa  *rsa.PrivateKey
	ec   *ecdsa.PrivateKey
	jwks *httptest.Server
}

var sharedRSA = sync.OnceValue(func() *rsa.PrivateKey { //nolint:gochecknoglobals // expensive key shared by tests
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	return k
})

func b64u(b *big.Int) string { return base64.RawURLEncoding.EncodeToString(b.Bytes()) }

func newSigner(t *testing.T) *signer {
	t.Helper()

	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	point, err := ec.PublicKey.Bytes()
	require.NoError(t, err)

	s := &signer{rsa: sharedRSA(), ec: ec}
	s.jwks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{
			{"kty": "RSA", "kid": "rsa-1", "n": b64u(s.rsa.N), "e": b64u(big.NewInt(int64(s.rsa.E)))},
			{
				"kty": "EC",
				"kid": "ec-1",
				"crv": "P-256",
				"x":   base64.RawURLEncoding.EncodeToString(point[1:33]),
				"y":   base64.RawURLEncoding.EncodeToString(point[33:]),
			},
		}})
	}))
	t.Cleanup(s.jwks.Close)

	return s
}

func (s *signer) token(t *testing.T, method jwt.SigningMethod, kid string, key any, claims jwt.MapClaims) string {
	t.Helper()

	tok := jwt.NewWithClaims(method, claims)
	tok.Header["kid"] = kid

	signed, err := tok.SignedString(key)
	require.NoError(t, err)

	return signed
}

func (s *signer) rsaToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	return s.token(t, jwt.SigningMethodRS256, "rsa-1", s.rsa, claims)
}

func jwtSpec(jwksURI, audiences string) string {
	def := "securityDefinitions:\n  jwt:\n    type: oauth2\n    flow: implicit\n    authorizationUrl: \"\"\n" +
		"    x-google-issuer: " + issuer + "\n    x-google-jwks_uri: " + jwksURI + "\n"
	if audiences != "" {
		def += "    x-google-audiences: " + audiences + "\n"
	}

	return def + "security:\n  - jwt: []\n"
}

func TestJWTSecurity(t *testing.T) {
	t.Parallel()

	now := newClock().Now()
	good := func(extra jwt.MapClaims) jwt.MapClaims {
		c := jwt.MapClaims{
			"iss": issuer, "sub": "user-1", "aud": "https://AUD", "exp": now.Add(time.Hour).Unix(),
			"iat": now.Unix(), "email": "u@example.com",
		}
		maps.Copy(c, extra)

		return c
	}

	tests := []struct {
		build func(*testing.T, *signer, string) (hdr map[string]string, target string)
		name  string
		aud   string
		msg   string
		want  int
	}{
		{
			name: "valid rs256", aud: "my-audience", want: 200,
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				return bearer(s.rsaToken(t, good(jwt.MapClaims{"aud": "my-audience"}))), "/hello"
			},
		},
		{
			name: "valid es256 with audience list", aud: "a1, a2", want: 200,
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				tok := s.token(
					t,
					jwt.SigningMethodES256,
					"ec-1",
					s.ec,
					good(jwt.MapClaims{"aud": []string{"zzz", "a2"}}),
				)

				return bearer(tok), "/hello"
			},
		},
		{
			name: "default audience is the managed service", want: 200,
			build: func(t *testing.T, s *signer, managed string) (map[string]string, string) {
				t.Helper()

				return bearer(s.rsaToken(t, good(jwt.MapClaims{"aud": "https://" + managed}))), "/hello"
			},
		},
		{
			name: "access_token query parameter", want: 200,
			build: func(t *testing.T, s *signer, managed string) (map[string]string, string) {
				t.Helper()

				return nil, "/hello?access_token=" + s.rsaToken(t, good(jwt.MapClaims{"aud": "https://" + managed}))
			},
		},
		{
			name: "missing token", want: 401, msg: "Jwt is missing",
			build: func(*testing.T, *signer, string) (map[string]string, string) { return nil, "/hello" },
		},
		{
			name:  "garbage token",
			want:  401,
			msg:   "Jwt verification fails",
			build: func(*testing.T, *signer, string) (map[string]string, string) { return bearer("not.a.jwt"), "/hello" },
		},
		{
			name: "token without dots", want: 401,
			msg: "Jwt is not in the form of Header.Payload.Signature with two dots and 3 sections",
			build: func(*testing.T, *signer, string) (map[string]string, string) {
				return bearer("garbage"), "/hello"
			},
		},
		{
			name: "not yet valid", aud: "x", want: 401, msg: "Jwt not yet valid",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				return bearer(
					s.rsaToken(t, good(jwt.MapClaims{"aud": "x", "nbf": now.Add(time.Hour).Unix()})),
				), "/hello"
			},
		},
		{
			name: "expired", aud: "x", want: 401, msg: "Jwt is expired",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				return bearer(
					s.rsaToken(t, good(jwt.MapClaims{"aud": "x", "exp": now.Add(-time.Hour).Unix()})),
				), "/hello"
			},
		},
		{
			name: "no exp", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				c := good(jwt.MapClaims{"aud": "x"})
				delete(c, "exp")

				return bearer(s.rsaToken(t, c)), "/hello"
			},
		},
		{
			name: "unknown issuer", aud: "x", want: 401, msg: "issuer is not configured",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				return bearer(s.rsaToken(t, good(jwt.MapClaims{"aud": "x", "iss": otherIs}))), "/hello"
			},
		},
		{
			name: "audience mismatch", aud: "mine", want: 403, msg: "Audiences in Jwt are not allowed",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				return bearer(s.rsaToken(t, good(jwt.MapClaims{"aud": "theirs"}))), "/hello"
			},
		},
		{
			name: "unknown kid", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				tok := s.token(t, jwt.SigningMethodRS256, "other", s.rsa, good(jwt.MapClaims{"aud": "x"}))

				return bearer(tok), "/hello"
			},
		},
		{
			name: "signed with another key", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				other, err := rsa.GenerateKey(rand.Reader, 2048)
				require.NoError(t, err)

				return bearer(
					s.token(t, jwt.SigningMethodRS256, "rsa-1", other, good(jwt.MapClaims{"aud": "x"})),
				), "/hello"
			},
		},
		{
			name: "hs256 forged with the public key", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				tok := s.token(t, jwt.SigningMethodHS256, "rsa-1", s.rsa.N.Bytes(), good(jwt.MapClaims{"aud": "x"}))

				return bearer(tok), "/hello"
			},
		},
		{
			name: "alg none", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				tok := s.token(
					t,
					jwt.SigningMethodNone,
					"rsa-1",
					jwt.UnsafeAllowNoneSignatureType,
					good(jwt.MapClaims{"aud": "x"}),
				)

				return bearer(tok), "/hello"
			},
		},
		{
			name: "es256 header with rsa key id", aud: "x", want: 401, msg: "Jwt verification fails",
			build: func(t *testing.T, s *signer, _ string) (map[string]string, string) {
				t.Helper()

				tok := s.token(t, jwt.SigningMethodES256, "rsa-1", s.ec, good(jwt.MapClaims{"aud": "x"}))

				return bearer(tok), "/hello"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSigner(t)
			be := echoBackend(t)
			e := newEnv(t)
			e.clock.Advance(0)

			e.createAPI(t, apiID)

			managed := e.managedService(t)
			cfg := e.createConfig(t, "cfg-1", opSpec(be.URL, jwtSpec(s.jwks.URL, tt.aud), "  /hello:\n"+op("get", "")))
			gw := e.createGateway(t, "my-gw", cfg.GetName())

			hdr, target := tt.build(t, s, managed)
			got := e.call(t, gw, http.MethodGet, target, hdr)
			require.Equal(t, tt.want, got.status, got.raw)

			if tt.msg != "" {
				assert.Contains(t, got.raw, tt.msg)
			}
		})
	}
}

func bearer(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

func TestJWTUserinfoForwarded(t *testing.T) {
	t.Parallel()

	s := newSigner(t)
	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, jwtSpec(s.jwks.URL, "aud-1"), "  /hello:\n"+op("get", "")))

	tok := s.rsaToken(t, jwt.MapClaims{
		"iss":   issuer,
		"sub":   "user-1",
		"aud":   "aud-1",
		"exp":   e.clock.Now().Add(time.Hour).Unix(),
		"email": "u@example.com",
	})

	got := e.call(t, gw, http.MethodGet, "/hello", map[string]string{
		"Authorization": "Bearer " + tok, "X-Apigateway-Api-Userinfo": "forged",
	})
	require.Equal(t, http.StatusOK, got.status, got.raw)

	hdr, ok := got.body["headers"].(map[string]any)
	require.True(t, ok)

	raw, err := base64.RawURLEncoding.DecodeString(fmt.Sprint(hdr["X-Apigateway-Api-Userinfo"]))
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(raw, &claims))
	assert.Equal(t, "user-1", claims["sub"])
	assert.Equal(t, "u@example.com", claims["email"])
	assert.Equal(t, "Bearer "+tok, hdr["X-Forwarded-Authorization"])
	assert.Empty(t, hdr["Authorization"])
}

func TestJWTExpiryFollowsClock(t *testing.T) {
	t.Parallel()

	s := newSigner(t)
	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, jwtSpec(s.jwks.URL, "aud-1"), "  /hello:\n"+op("get", "")))

	tok := s.rsaToken(t, jwt.MapClaims{
		"iss": issuer, "aud": "aud-1", "exp": e.clock.Now().Add(time.Minute).Unix(),
	})

	assert.Equal(t, http.StatusOK, e.call(t, gw, http.MethodGet, "/hello", bearer(tok)).status)

	e.clock.Advance(2 * time.Minute)
	assert.Equal(t, http.StatusUnauthorized, e.call(t, gw, http.MethodGet, "/hello", bearer(tok)).status)
}

func TestJWKSFetchRefusedOutsideLoopback(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, jwtSpec("http://192.0.2.1:9/keys", "aud-1"), "  /hello:\n"+op("get", "")))

	tok := (&signer{rsa: sharedRSA()}).rsaToken(t, jwt.MapClaims{
		"iss": issuer, "aud": "aud-1", "exp": e.clock.Now().Add(time.Hour).Unix(),
	})

	got := e.call(t, gw, http.MethodGet, "/hello", bearer(tok))
	assert.Equal(t, http.StatusUnauthorized, got.status, "a JWKS URI outside loopback cannot be fetched")
	assert.EqualValues(t, "Jwks remote fetch is failed", got.body["message"])
}

func TestJWKSWithoutUsableKeys(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	t.Cleanup(empty.Close)

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, jwtSpec(empty.URL, "aud-1"), "  /hello:\n"+op("get", "")))

	tok := (&signer{rsa: sharedRSA()}).rsaToken(t, jwt.MapClaims{
		"iss": issuer, "aud": "aud-1", "exp": e.clock.Now().Add(time.Hour).Unix(),
	})

	got := e.call(t, gw, http.MethodGet, "/hello", bearer(tok))
	assert.Equal(t, http.StatusUnauthorized, got.status)
	assert.EqualValues(t, "Jwks doesn't have any valid public key", got.body["message"])
}

func TestQuotaPerMinute(t *testing.T) {
	t.Parallel()

	const top = "x-google-management:\n  metrics:\n    - name: reads\n      displayName: Reads\n      valueType: INT64\n" +
		"      metricKind: DELTA\n  quota:\n    limits:\n      - name: reads-limit\n        metric: reads\n" +
		"        unit: \"1/min/{project}\"\n        values:\n          STANDARD: 5\n"

	paths := "  /read:\n" + op("get", "      x-google-quota:\n        metricCosts:\n          reads: 2\n") +
		"  /free:\n" + op("get", "")

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, opSpec(be.URL, top, paths))

	statuses := func(n int, target string) []int {
		out := make([]int, 0, n)
		for range n {
			out = append(out, e.call(t, gw, http.MethodGet, target, nil).status)
		}

		return out
	}

	assert.Equal(t, []int{200, 200, 429, 429}, statuses(4, "/read"), "cost 2 against 5 per minute")

	denied := e.call(t, gw, http.MethodGet, "/read", nil)
	assert.Contains(t, denied.raw, "RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'reads'")
	assert.Equal(t, []int{200, 200, 200}, statuses(3, "/free"), "operations without a cost are not limited")

	e.clock.Advance(time.Minute)
	assert.Equal(t, []int{200, 200, 429}, statuses(3, "/read"), "the window resets each minute")
}
