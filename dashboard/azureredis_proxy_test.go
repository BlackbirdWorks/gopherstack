package dashboard_test

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

const (
	redisPxBase  = "/dashboard/api/v1/azure/redis"
	redisCacheID = "/subscriptions/s/resourcegroups/rg/providers/microsoft.cache/redis/cache1"
)

type redisEnv struct {
	h    *dashboard.DashboardHandler
	keys azureredis.Keys
}

func newRedisEnv(t *testing.T) *redisEnv {
	t.Helper()

	b := azureredis.NewInMemoryBackend(azureredis.DefaultSettings())
	t.Cleanup(b.Close)

	ep, err := b.Ensure(redisCacheID, azureredis.Spec{
		SKU: azureredis.SKUStandard, MinTLS: "1.2", EnableNonSSLPort: true,
	})
	require.NoError(t, err)

	keys, err := b.Keys(redisCacheID)
	require.NoError(t, err)

	c := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ep.Host, strconv.Itoa(ep.Port)), Password: keys.Primary,
	})
	t.Cleanup(func() { _ = c.Close() })

	ctx := t.Context()
	require.NoError(t, c.Set(ctx, "user:1", "alice", 0).Err())
	require.NoError(t, c.Set(ctx, "user:2", "bob", 0).Err())
	require.NoError(t, c.Set(ctx, "session:9", strings.Repeat("x", 6000), 0).Err())
	require.NoError(t, c.RPush(ctx, "queue", "a", "b", "c").Err())
	require.NoError(t, c.HSet(ctx, "profile", "name", "alice", "city", "x").Err())
	require.NoError(t, c.ZAdd(ctx, "board", goredis.Z{Score: 2, Member: "m"}).Err())
	require.NoError(t, c.Set(ctx, "ttl:key", "v", 0).Err())
	require.NoError(t, c.Expire(ctx, "ttl:key", 3600*1e9).Err())

	c3 := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ep.Host, strconv.Itoa(ep.Port)), Password: keys.Primary, DB: 3,
	})
	t.Cleanup(func() { _ = c3.Close() })
	require.NoError(t, c3.Set(ctx, "other", "db3", 0).Err())

	return &redisEnv{
		h:    dashboard.NewHandler(dashboard.Config{AzureRedisOps: azureredis.NewHandler(b)}),
		keys: keys,
	}
}

func redisURL(path string, q url.Values) string {
	if q == nil {
		q = url.Values{}
	}

	if q.Get("cache") == "" {
		q.Set("cache", redisCacheID)
	}

	return redisPxBase + path + "?" + q.Encode()
}

func TestAzureRedisProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantBody []string
		notBody  []string
		wantCode int
	}{
		{
			name: "caches", path: redisPxBase + "/caches", wantCode: 200,
			wantBody: []string{`"name":"cache1"`, `"sku":"Standard"`, `"enableNonSslPort":true`},
		},
		{
			name: "scan all", path: redisURL("/keys", nil), wantCode: 200,
			wantBody: []string{`"user:1"`, `"queue"`, `"type":"list"`},
		},
		{
			name: "scan pattern", path: redisURL("/keys", url.Values{"match": {"user:*"}}), wantCode: 200,
			wantBody: []string{`"user:1"`, `"user:2"`}, notBody: []string{`"queue"`},
		},
		{
			name: "scan db3", path: redisURL("/keys", url.Values{"db": {"3"}}), wantCode: 200,
			wantBody: []string{`"other"`}, notBody: []string{`"user:1"`},
		},
		{
			name: "scan pages", path: redisURL("/keys", url.Values{"count": {"2"}}), wantCode: 200,
			wantBody: []string{`"next":"`},
		},
		{
			name: "ttl", path: redisURL("/keys", url.Values{"match": {"ttl:*"}}), wantCode: 200,
			wantBody: []string{`"ttlSeconds":3600`},
		},
		{
			name: "no expiry", path: redisURL("/keys", url.Values{"match": {"user:1"}}), wantCode: 200,
			wantBody: []string{`"ttlSeconds":-1`},
		},
		{
			name: "string value", path: redisURL("/value", url.Values{"key": {"user:1"}}), wantCode: 200,
			wantBody: []string{`"text":"alice"`, `"type":"string"`},
		},
		{
			name: "capped string", path: redisURL("/value", url.Values{"key": {"session:9"}}), wantCode: 200,
			wantBody: []string{`"truncated":true`, `"total":6000`},
		},
		{
			name: "list value", path: redisURL("/value", url.Values{"key": {"queue"}}), wantCode: 200,
			wantBody: []string{`"elements":["a","b","c"]`},
		},
		{
			name: "hash value", path: redisURL("/value", url.Values{"key": {"profile"}}), wantCode: 200,
			wantBody: []string{`city = x`, `name = alice`},
		},
		{
			name: "zset value", path: redisURL("/value", url.Values{"key": {"board"}}), wantCode: 200,
			wantBody: []string{`m (score 2)`},
		},
		{
			name: "missing key", path: redisURL("/value", url.Values{"key": {"nope"}}), wantCode: 404,
			wantBody: []string{"NOT_FOUND"},
		},
		{
			name: "bad database", path: redisURL("/keys", url.Values{"db": {"16"}}), wantCode: 400,
			wantBody: []string{"database must be"},
		},
		{
			name: "unknown cache", path: redisURL("/keys", url.Values{"cache": {"/x/y"}}), wantCode: 404,
			wantBody: []string{"NOT_FOUND"},
		},
		{
			name: "missing cache", path: redisPxBase + "/keys", wantCode: 400,
			wantBody: []string{"cache is required"},
		},
		{
			name: "long pattern", path: redisURL("/keys", url.Values{"match": {strings.Repeat("a", 300)}}), wantCode: 400,
			wantBody: []string{"too long"},
		},
		{
			name: "missing key name", path: redisURL("/value", nil), wantCode: 400,
			wantBody: []string{"key is required"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRedisEnv(t)
			rec := doProxy(t, env.h, http.MethodGet, tt.path, "")

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			for _, w := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), w)
			}

			for _, w := range tt.notBody {
				assert.NotContains(t, rec.Body.String(), w)
			}
		})
	}
}

func TestAzureRedisKeysNeverInGets(t *testing.T) {
	t.Parallel()

	env := newRedisEnv(t)

	for _, path := range []string{redisPxBase + "/caches", redisURL("/keys", nil), redisURL("/value", url.Values{"key": {"user:1"}})} {
		rec := doProxy(t, env.h, http.MethodGet, path, "")
		assert.NotContains(t, rec.Body.String(), env.keys.Primary, path)
		assert.NotContains(t, rec.Body.String(), env.keys.Secondary, path)
	}

	rec := doProxy(t, env.h, http.MethodGet, redisPxBase+"/access-keys/reveal", "")
	assert.NotContains(t, rec.Body.String(), env.keys.Primary)
}

func TestAzureRedisReveal(t *testing.T) {
	t.Parallel()

	env := newRedisEnv(t)

	rec := doProxy(t, env.h, http.MethodPost, redisPxBase+"/access-keys/reveal", `{"cache":"`+redisCacheID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, env.keys.Primary, got["primary"])
	assert.Equal(t, env.keys.Secondary, got["secondary"])

	rec = doProxy(t, env.h, http.MethodPost, redisPxBase+"/access-keys/reveal", `{"cache":"/nope"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doProxy(t, env.h, http.MethodPost, redisPxBase+"/access-keys/reveal", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAzureRedisIsReadOnly(t *testing.T) {
	t.Parallel()

	env := newRedisEnv(t)

	for _, m := range []string{http.MethodPut, http.MethodDelete, http.MethodPost} {
		rec := doProxy(t, env.h, m, redisURL("/keys", nil), `{"key":"x"}`)
		assert.NotEqual(t, http.StatusOK, rec.Code, m)
	}

	rec := doProxy(t, env.h, http.MethodGet, redisURL("/keys", url.Values{"match": {"user:*"}}), "")
	assert.Contains(t, rec.Body.String(), `"user:1"`)
}

func TestAzureRedisAbsent(t *testing.T) {
	t.Parallel()

	rec := doProxy(t, dashboard.NewHandler(dashboard.Config{}), http.MethodGet, redisPxBase+"/caches", "")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "UNAVAILABLE")
}
