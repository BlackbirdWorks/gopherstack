package ociapigateway_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cacheSecretID = "ocid1.vaultsecret.oc1..cache"

func (e *env) startCacheServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()

	mr := miniredis.RunT(t)
	mr.RequireUserAuth("cacheuser", "cachepw")
	e.backend.SetSecretResolver(func(id string, version int64) (string, bool) {
		if id != cacheSecretID || version != 1 {
			return "", false
		}

		return `{"username":"cacheuser","password":"cachepw"}`, true
	})

	return mr
}

func (e *env) mkCachedGateway(t *testing.T, name string, mr *miniredis.Miniredis) apigateway.Gateway {
	t.Helper()

	created, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
		CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
		SubnetId: new(testSubnet), DisplayName: new(name),
		ResponseCacheDetails: apigateway.ExternalRespCache{
			Servers: []apigateway.ResponseCacheRespServer{
				{Host: new(mr.Host()), Port: new(mr.Server().Addr().Port)},
			},
			AuthenticationSecretId:            new(cacheSecretID),
			AuthenticationSecretVersionNumber: new(int64(1)),
		},
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.gw.GetGateway(t.Context(), apigateway.GetGatewayRequest{GatewayId: created.Id})
	require.NoError(t, err)

	ext, ok := got.ResponseCacheDetails.(apigateway.ExternalRespCache)
	require.True(t, ok, "responseCacheDetails round-trips as EXTERNAL_RESP_CACHE")
	assert.Equal(t, mr.Host(), *ext.Servers[0].Host)

	return got.Gateway
}

func cachedSpec(backendURL, lookupExtra string) string {
	const ttl = 60

	return `{"routes":[{"path":"/c","methods":["GET"],"backend":{"type":"HTTP_BACKEND","url":"` + backendURL + `"},` +
		`"requestPolicies":{"responseCacheLookup":{"type":"SIMPLE_LOOKUP_POLICY","isEnabled":true` + lookupExtra + `}},` +
		`"responsePolicies":{"responseCacheStore":{"type":"FIXED_TTL_STORE_POLICY","timeToLiveInSeconds":` +
		strconv.Itoa(ttl) + `}}}]}`
}

func TestResponseCache(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("X-Origin-Hit", strconv.Itoa(int(n)))
		_, _ = w.Write([]byte("v" + strconv.Itoa(int(n)) + ":" + r.Header.Get("Accept")))
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	mr := e.startCacheServer(t)
	gw := e.mkCachedGateway(t, "cached", mr)
	e.mkDeployment(t, *gw.Id, "/k", cachedSpec(backend.URL, `,"cacheKeyAdditions":["request.headers[Accept]"]`))

	first := e.call(t, *gw.Id, http.MethodGet, "/k/c?ignored=1", map[string]string{"Accept": "a/b"}, "")
	assert.Equal(t, "MISS", first.header.Get("X-Cache-Status"))
	assert.Equal(t, "v1:a/b", first.body)

	second := e.call(t, *gw.Id, http.MethodGet, "/k/c?ignored=2", map[string]string{"Accept": "a/b"}, "")
	assert.Equal(t, "HIT", second.header.Get("X-Cache-Status"), "query parameters are not part of the default key")
	assert.Equal(t, "v1:a/b", second.body)
	assert.Equal(t, "1", second.header.Get("X-Origin-Hit"))

	other := e.call(t, *gw.Id, http.MethodGet, "/k/c", map[string]string{"Accept": "c/d"}, "")
	assert.Equal(t, "MISS", other.header.Get("X-Cache-Status"), "cacheKeyAdditions split the key")
	assert.Equal(t, "v2:c/d", other.body)

	e.clock.Advance(59 * time.Second)
	assert.Equal(t, "HIT", e.call(t, *gw.Id, http.MethodGet, "/k/c", map[string]string{"Accept": "a/b"}, "").
		header.Get("X-Cache-Status"))

	e.clock.Advance(2 * time.Second)

	expired := e.call(t, *gw.Id, http.MethodGet, "/k/c", map[string]string{"Accept": "a/b"}, "")
	assert.Equal(t, "MISS", expired.header.Get("X-Cache-Status"), "entry expires after the fixed TTL on the clock")
	assert.Equal(t, "v3:a/b", expired.body)
	assert.Equal(t, int32(3), hits.Load())
}

func TestResponseCacheRules(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)

		if r.URL.Query().Get("fail") != "" {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	mr := e.startCacheServer(t)
	gw := e.mkCachedGateway(t, "cached-rules", mr)
	e.mkDeployment(t, *gw.Id, "/pub", cachedSpec(backend.URL, ""))
	e.mkDeployment(t, *gw.Id, "/priv", cachedSpec(backend.URL, `,"isPrivateCachingEnabled":true`))

	status := func(prefix, method string, h map[string]string, q string) string {
		return e.call(t, *gw.Id, method, prefix+"/c"+q, h, "").header.Get("X-Cache-Status")
	}

	auth := map[string]string{"Authorization": "Bearer t"}

	assert.Equal(
		t,
		"BYPASS",
		status("/pub", http.MethodGet, auth, ""),
		"credentialed requests skip the cache by default",
	)
	assert.Equal(t, "BYPASS", status("/pub", http.MethodGet, auth, ""))
	assert.Equal(t, "MISS", status("/priv", http.MethodGet, auth, ""))
	assert.Equal(t, "HIT", status("/priv", http.MethodGet, auth, ""), "isPrivateCachingEnabled caches them")

	assert.Equal(t, "MISS", status("/pub", http.MethodGet, nil, "?fail=1"))
	assert.Equal(t, "MISS", status("/pub", http.MethodGet, nil, "?fail=1"), "5xx responses are never stored")

	assert.Empty(t, status("/pub", http.MethodPost, nil, ""), "only GET, HEAD and OPTIONS are cached")
}

func TestResponseCacheNeedsGatewayCache(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "uncached")
	e.mkDeployment(t, *gw.Id, "/k", cachedSpec(backend.URL, ""))

	assert.Equal(t, "BYPASS", e.call(t, *gw.Id, http.MethodGet, "/k/c", nil, "").header.Get("X-Cache-Status"))
}

func TestGatewayResponseCacheValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	tests := []struct {
		detail apigateway.ResponseCacheDetails
		name   string
	}{
		{
			name: "no servers",
			detail: apigateway.ExternalRespCache{
				Servers:                           []apigateway.ResponseCacheRespServer{},
				AuthenticationSecretId:            new("s"),
				AuthenticationSecretVersionNumber: new(int64(1)),
			},
		},
		{name: "two servers", detail: apigateway.ExternalRespCache{
			Servers: []apigateway.ResponseCacheRespServer{
				{Host: new("h"), Port: new(1)}, {Host: new("i"), Port: new(2)},
			},
			AuthenticationSecretId: new("s"), AuthenticationSecretVersionNumber: new(int64(1)),
		}},
		{name: "zero timeout", detail: apigateway.ExternalRespCache{
			Servers:                []apigateway.ResponseCacheRespServer{{Host: new("h"), Port: new(1)}},
			AuthenticationSecretId: new("s"), AuthenticationSecretVersionNumber: new(int64(1)),
			ReadTimeoutInMs: new(0),
		}},
		{name: "no secret", detail: apigateway.ExternalRespCache{
			Servers: []apigateway.ResponseCacheRespServer{{Host: new("h"), Port: new(1)}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
				CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
				SubnetId: new(testSubnet), ResponseCacheDetails: tt.detail,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestResponseCacheUsesCacheServer(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	mr := e.startCacheServer(t)
	gw := e.mkCachedGateway(t, "stored", mr)
	e.mkDeployment(t, *gw.Id, "/k", cachedSpec(backend.URL, ""))

	assert.Equal(t, "MISS", e.call(t, *gw.Id, http.MethodGet, "/k/c", nil, "").header.Get("X-Cache-Status"))

	keys := mr.Keys()
	require.Len(t, keys, 1, "the response is written to the cache server")
	assert.Equal(t, 60*time.Second, mr.TTL(keys[0]))

	mr.FlushAll()

	assert.Equal(t, "MISS", e.call(t, *gw.Id, http.MethodGet, "/k/c", nil, "").header.Get("X-Cache-Status"),
		"entries live on the cache server, not in the gateway")
}

func TestResponseCacheOutage(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(backend.Close)

	tests := []struct {
		prepare func(e *env, mr *miniredis.Miniredis)
		name    string
	}{
		{name: "server down", prepare: func(_ *env, mr *miniredis.Miniredis) { mr.Close() }},
		{name: "secret unreadable", prepare: func(e *env, _ *miniredis.Miniredis) {
			e.backend.SetSecretResolver(func(string, int64) (string, bool) { return "", false })
		}},
		{name: "wrong password", prepare: func(e *env, _ *miniredis.Miniredis) {
			e.backend.SetSecretResolver(func(string, int64) (string, bool) { return `{"password":"nope"}`, true })
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			mr := e.startCacheServer(t)
			gw := e.mkCachedGateway(t, "outage", mr)
			e.mkDeployment(t, *gw.Id, "/k", cachedSpec(backend.URL, ""))
			tt.prepare(e, mr)

			res := e.call(t, *gw.Id, http.MethodGet, "/k/c", nil, "")
			assert.Equal(t, http.StatusOK, res.status, "the request is answered by the back end")
			assert.Equal(t, "body", res.body)
			assert.Equal(t, "BYPASS", res.header.Get("X-Cache-Status"))
		})
	}
}

func TestResponseCacheClientReleasedWithGateway(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	mr := e.startCacheServer(t)
	gw := e.mkCachedGateway(t, "released", mr)
	dep := e.mkDeployment(t, *gw.Id, "/k", cachedSpec(backend.URL, ""))

	e.call(t, *gw.Id, http.MethodGet, "/k/c", nil, "")
	require.Positive(t, mr.CurrentConnectionCount())

	_, err := e.dep.DeleteDeployment(t.Context(), apigateway.DeleteDeploymentRequest{DeploymentId: &dep})
	require.NoError(t, err)
	e.settle()

	_, err = e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: gw.Id})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return mr.CurrentConnectionCount() == 0 }, 5*time.Second, 10*time.Millisecond)
}
