package azurearm_test

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMGateway_ValidateJWTEmptyClaimMatches(t *testing.T) {
	t.Parallel()

	secret := []byte("0123456789abcdef0123456789abcdef")
	key := base64.StdEncoding.EncodeToString(secret)

	tests := []struct {
		claims jwt.MapClaims
		name   string
		policy string
	}{
		{jwt.MapClaims{}, "missing aud, empty audience", `<audiences><audience></audience></audiences>`},
		{jwt.MapClaims{}, "missing iss, empty issuer", `<issuers><issuer></issuer></issuers>`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="X-T">`+signingKey(key)+tt.policy+
				`</validate-jwt>`), http.StatusCreated)

			tt.claims["exp"] = e.clock.Now().Add(time.Hour).Unix()
			tok := signHS(t, jwt.SigningMethodHS256, secret, tt.claims)

			status, body, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": tok})
			assert.Equal(t, http.StatusUnauthorized, status, body)
		})
	}
}

func TestAPIMGateway_ValidateJWTMalformedHidesParserText(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="X-T">`+signingKey(key)+`</validate-jwt>`),
		http.StatusCreated)

	tests := []struct{ name, tok string }{
		{"two segments", "a.b"},
		{"bad base64", "!!!.@@@.###"},
		{"non json header", "aGVsbG8.aGVsbG8.c2ln"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": tt.tok})
			assert.Equal(t, http.StatusUnauthorized, status, body)
			assert.NotContains(t, body, "illegal base64")
			assert.NotContains(t, body, "invalid character")
			assert.NotContains(t, body, "token is malformed")
		})
	}
}

func TestAPIMGateway_ValidateJWTFailedConfigFetchIsCached(t *testing.T) {
	t.Parallel()

	var hits atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", jwtPolicy(`<validate-jwt header-name="X-T"><openid-config url="`+srv.URL+
		`/.well-known/openid-configuration" /></validate-jwt>`), http.StatusCreated)

	for range 5 {
		status, _, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": "a.b.c"})
		require.Equal(t, http.StatusUnauthorized, status)
	}

	assert.Equal(t, int64(1), hits.Load(), "a failing configuration endpoint is not refetched per request")

	e.clock.Advance(6 * time.Minute)

	status, _, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": "a.b.c"})
	require.Equal(t, http.StatusUnauthorized, status)
	assert.Equal(t, int64(2), hits.Load(), "the retry interval elapsed")
}
