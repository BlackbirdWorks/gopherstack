package apigatewayv2

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doInternalRequest performs an HTTP request against h and returns the
// recorder. It mirrors handler_test.go's doRequest, duplicated here (rather
// than exported) because this file lives in the internal `apigatewayv2`
// package specifically to reach the unexported authCache field.
func doInternalRequest(t *testing.T, h *Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)

	require.NoError(t, h.Handler()(c))

	return rr
}

// TestAuthorizerCache_Purge verifies that purge removes every cached
// decision for the given authorizer ID (across all identity-source values)
// while leaving other authorizers' cached entries untouched.
func TestAuthorizerCache_Purge(t *testing.T) {
	t.Parallel()

	c := newAuthorizerCache()
	c.put("auth-1\nidentity-a", true, time.Minute)
	c.put("auth-1\nidentity-b", false, time.Minute)
	c.put("auth-2\nidentity-a", true, time.Minute)

	c.purge("auth-1")

	_, ok := c.get("auth-1\nidentity-a")
	assert.False(t, ok, "auth-1/identity-a should be purged")

	_, ok = c.get("auth-1\nidentity-b")
	assert.False(t, ok, "auth-1/identity-b should be purged")

	allow, ok := c.get("auth-2\nidentity-a")
	require.True(t, ok, "auth-2's entry must survive purging auth-1")
	assert.True(t, allow)
}

// TestAuthorizerCache_Purge_NoMatch confirms purging an authorizer id with no
// cached entries is a safe no-op that leaves unrelated entries alone.
func TestAuthorizerCache_Purge_NoMatch(t *testing.T) {
	t.Parallel()

	c := newAuthorizerCache()
	c.put("auth-1\nidentity-a", true, time.Minute)

	c.purge("does-not-exist")

	allow, ok := c.get("auth-1\nidentity-a")
	require.True(t, ok)
	assert.True(t, allow)
}

// TestHandler_DeleteAuthorizer_PurgesCache verifies that DeleteAuthorizer
// purges cached decisions for that authorizer (bd: gopherstack-wmh), so a
// stale allow/deny decision can't leak past deletion until its TTL expires.
func TestHandler_DeleteAuthorizer_PurgesCache(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	h := NewHandler(b)

	api, err := b.CreateAPI(context.Background(), CreateAPIInput{Name: "api", ProtocolType: protocolTypeHTTP})
	require.NoError(t, err)

	auth, err := b.CreateAuthorizer(api.APIID, CreateAuthorizerInput{
		Name:           "req-auth",
		AuthorizerType: authorizerTypeRequest,
		AuthorizerURI:  "arn:aws:lambda:us-east-1:123456789012:function:auth-fn",
	})
	require.NoError(t, err)

	cacheKey := auth.AuthorizerID + "\nsome-identity"
	h.authCache.put(cacheKey, true, time.Minute)

	_, ok := h.authCache.get(cacheKey)
	require.True(t, ok, "precondition: decision must be cached before delete")

	rr := doInternalRequest(t, h, http.MethodDelete, "/v2/apis/"+api.APIID+"/authorizers/"+auth.AuthorizerID, nil)
	require.Equal(t, http.StatusNoContent, rr.Code)

	_, ok = h.authCache.get(cacheKey)
	assert.False(t, ok, "cached decision must be purged on DeleteAuthorizer")
}

// TestHandler_DeleteAPI_PurgesAuthorizerCache verifies that DeleteApi purges
// cached decisions for every authorizer that belonged to the deleted API
// (bd: gopherstack-wmh) rather than leaving them to self-heal via TTL.
func TestHandler_DeleteAPI_PurgesAuthorizerCache(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	h := NewHandler(b)

	api, err := b.CreateAPI(context.Background(), CreateAPIInput{Name: "api", ProtocolType: protocolTypeHTTP})
	require.NoError(t, err)

	auth, err := b.CreateAuthorizer(api.APIID, CreateAuthorizerInput{
		Name:           "req-auth",
		AuthorizerType: authorizerTypeRequest,
		AuthorizerURI:  "arn:aws:lambda:us-east-1:123456789012:function:auth-fn",
	})
	require.NoError(t, err)

	cacheKey := auth.AuthorizerID + "\nsome-identity"
	h.authCache.put(cacheKey, true, time.Minute)

	rr := doInternalRequest(t, h, http.MethodDelete, "/v2/apis/"+api.APIID, nil)
	require.Equal(t, http.StatusNoContent, rr.Code)

	_, ok := h.authCache.get(cacheKey)
	assert.False(t, ok, "cached decision must be purged when the owning API is deleted")
}

type arnPolicyInvoker struct {
	allowARN string
	calls    atomic.Int32
}

func (p *arnPolicyInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	p.calls.Add(1)

	body := `{"principalId":"u","policyDocument":{"Statement":[` +
		`{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"` + p.allowARN + `"}]}}`

	return []byte(body), 200, nil
}

func TestEnforceRequestAuthorizer_CacheKeyedByIdentityAndRoute(t *testing.T) {
	t.Parallel()

	const armA = "arn:aws:execute-api:us-east-1:000000000000:api1/prod/GET/a"

	type call struct {
		header  string
		path    string
		wantErr bool
	}

	tests := []struct {
		name       string
		calls      []call
		wantInvoke int32
	}{
		{
			name: "cached_allow_for_one_route_does_not_allow_another",
			calls: []call{
				{header: "alice", path: "/a"},
				{header: "alice", path: "/b", wantErr: true},
				{header: "alice", path: "/a"},
			},
			wantInvoke: 2,
		},
		{
			name: "missing_identity_source_401_without_invoke",
			calls: []call{
				{header: "", path: "/a", wantErr: true},
			},
			wantInvoke: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &arnPolicyInvoker{allowARN: armA}
			h := NewHandler(NewInMemoryBackend())
			h.lambdaInvoker = inv
			auth := &Authorizer{
				AuthorizerID: "a1", AuthorizerType: authorizerTypeRequest,
				IdentitySource:               []string{"$request.header.Auth"},
				AuthorizerResultTTLInSeconds: 300,
			}

			for i, c := range tt.calls {
				req := httptest.NewRequest(http.MethodGet, c.path, nil)
				if c.header != "" {
					req.Header.Set("Auth", c.header)
				}

				ctx := echo.New().NewContext(req, httptest.NewRecorder())
				err := h.enforceRequestAuthorizer(ctx, "api1", "prod", &Route{RouteKey: "GET " + c.path}, auth, c.path)
				assert.Equal(t, c.wantErr, err != nil, "call %d", i)
			}

			assert.Equal(t, tt.wantInvoke, inv.calls.Load())
		})
	}
}

func TestAuthorizerCache_Bounded(t *testing.T) {
	t.Parallel()

	c := newAuthorizerCache()
	for i := range maxAuthorizerCacheEntries + 50 {
		c.put(strconv.Itoa(i), true, time.Hour)
	}

	assert.LessOrEqual(t, len(c.m), maxAuthorizerCacheEntries)
}

func TestAuthorizerCache_TTLExpiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newAuthorizerCache()
		c.put("k", true, time.Minute)
		time.Sleep(time.Minute + time.Second)

		_, hit := c.get("k")
		assert.False(t, hit)
	})
}
