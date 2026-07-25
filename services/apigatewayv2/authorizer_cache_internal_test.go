package apigatewayv2

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
