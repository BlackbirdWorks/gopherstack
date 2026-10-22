package azurearm_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	redisBase = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg1/providers/Microsoft.Cache/redis/"
	redisAPI  = "?api-version=2024-11-01"
)

var errRedisBoom = errors.New("boom")

type fakeRedis struct {
	specs    map[string]azurearm.RedisSpec
	keys     map[string]azurearm.RedisKeys
	calls    []string
	mu       sync.Mutex
	failNext bool
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{specs: map[string]azurearm.RedisSpec{}, keys: map[string]azurearm.RedisKeys{}}
}

func (f *fakeRedis) Ensure(id string, spec azurearm.RedisSpec) (azurearm.RedisEndpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failNext {
		f.failNext = false

		return azurearm.RedisEndpoint{}, errRedisBoom
	}

	f.specs[id] = spec
	if _, ok := f.keys[id]; !ok {
		f.keys[id] = azurearm.RedisKeys{Primary: "pk", Secondary: "sk"}
	}

	return f.endpointLocked(id), nil
}

func (f *fakeRedis) endpointLocked(id string) azurearm.RedisEndpoint {
	ep := azurearm.RedisEndpoint{Host: "127.0.0.1", SSLPort: 6380}
	if f.specs[id].EnableNonSSLPort {
		ep.Port = 6379
	}

	return ep
}

func (f *fakeRedis) Endpoint(id string) (azurearm.RedisEndpoint, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.specs[id]; !ok {
		return azurearm.RedisEndpoint{}, false
	}

	return f.endpointLocked(id), true
}

func (f *fakeRedis) Delete(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.specs, id)
	f.calls = append(f.calls, "delete")

	return nil
}

func (f *fakeRedis) Keys(id string) (azurearm.RedisKeys, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.keys[id], nil
}

func (f *fakeRedis) RegenerateKey(id, keyType string) (azurearm.RedisKeys, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := f.keys[id]
	if strings.EqualFold(keyType, "Primary") {
		k.Primary = "pk2"
	} else {
		k.Secondary = "sk2"
	}

	f.keys[id] = k

	return k, nil
}

func (f *fakeRedis) Reboot(_, rebootType string) error {
	f.record("reboot:" + rebootType)

	return nil
}

func (f *fakeRedis) Import(_, format string, files []string) error {
	f.record("import:" + format + ":" + strings.Join(files, ","))

	return nil
}

func (f *fakeRedis) Export(_, format, prefix, container string) error {
	f.record("export:" + format + ":" + prefix + ":" + container)

	return nil
}

func (f *fakeRedis) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, s)
}

type redisClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *redisClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *redisClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type redisEnv struct {
	h     *azurearm.Handler
	p     *azurearm.RedisProvider
	dp    *fakeRedis
	clock *redisClock
}

func newRedisEnv(t *testing.T) *redisEnv {
	t.Helper()

	dp := newFakeRedis()
	clock := &redisClock{t: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	h, p := azurearm.NewTestHandlerWithRedis(dp, clock.now)

	return &redisEnv{h: h, p: p, dp: dp, clock: clock}
}

func (e *redisEnv) do(t *testing.T, method, path string, body any) (int, map[string]any, http.Header) {
	t.Helper()

	var rdr *bytes.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)

		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, rdr)
	req.Host = "host:10006"
	rec := httptest.NewRecorder()
	newEchoServer(e.h).ServeHTTP(rec, req)

	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}

	return rec.Code, out, rec.Header()
}

func redisCacheBody(sku, family string, capacity int, props map[string]any) map[string]any {
	p := map[string]any{"sku": map[string]any{"name": sku, "family": family, "capacity": capacity}}
	maps.Copy(p, props)

	return map[string]any{"location": "local", "properties": p}
}

func propsOf(body map[string]any) map[string]any {
	p, _ := body["properties"].(map[string]any)

	return p
}

func TestRedisCreateLifecycle(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, body, hdr := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	require.Equal(t, http.StatusCreated, status, body)
	assert.Equal(t, "Creating", propsOf(body)["provisioningState"])
	assert.Equal(t, "Microsoft.Cache/Redis", body["type"])
	assert.Equal(t, "c1.redis.cache.windows.net", propsOf(body)["hostName"])
	assert.EqualValues(t, 6380, propsOf(body)["sslPort"])
	assert.NotContains(t, propsOf(body), "port", "no plaintext port while enableNonSslPort is false")
	assert.Equal(t, "1.2", propsOf(body)["minimumTlsVersion"])
	assert.Equal(t, "Enabled", propsOf(body)["publicNetworkAccess"])

	cfg, _ := propsOf(body)["redisConfiguration"].(map[string]any)
	assert.Equal(t, "volatile-lru", cfg["maxmemory-policy"])
	assert.Equal(t, "256", cfg["maxclients"])

	op := hdr.Get("Azure-Asyncoperation")
	require.Contains(t, op, "/providers/Microsoft.Cache/locations/")

	opURL, err := url.Parse(op)
	require.NoError(t, err)

	opPath := opURL.RequestURI()
	_, st, _ := e.do(t, http.MethodGet, opPath, nil)
	assert.Equal(t, "InProgress", st["status"])

	e.clock.advance(time.Second)

	_, st, _ = e.do(t, http.MethodGet, opPath, nil)
	assert.Equal(t, "Succeeded", st["status"])

	_, got, _ := e.do(t, http.MethodGet, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, "Succeeded", propsOf(got)["provisioningState"])

	status, body, _ = e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	assert.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "Succeeded", propsOf(body)["provisioningState"])
}

func TestRedisCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
		want int
	}{
		{name: "no sku", path: "c1", body: map[string]any{"properties": map[string]any{}}, want: http.StatusBadRequest},
		{name: "bad sku name", path: "c1", body: redisCacheBody("Gold", "C", 0, nil), want: http.StatusBadRequest},
		{
			name: "premium needs P", path: "c1", want: http.StatusBadRequest,
			body: redisCacheBody("Premium", "C", 1, nil),
		},
		{
			name: "premium capacity 0",
			path: "c1",
			body: redisCacheBody("Premium", "P", 0, nil),
			want: http.StatusBadRequest,
		},
		{
			name: "standard capacity 7",
			path: "c1",
			body: redisCacheBody("Standard", "C", 7, nil),
			want: http.StatusBadRequest,
		},
		{
			name: "bad tls", path: "c1", want: http.StatusBadRequest,
			body: redisCacheBody("Basic", "C", 0, map[string]any{"minimumTlsVersion": "1.3"}),
		},
		{
			name: "bad public access", path: "c1", want: http.StatusBadRequest,
			body: redisCacheBody("Basic", "C", 0, map[string]any{"publicNetworkAccess": "Maybe"}),
		},
		{name: "double hyphen", path: "a--b", body: redisCacheBody("Basic", "C", 0, nil), want: http.StatusBadRequest},
		{name: "leading hyphen", path: "-ab", body: redisCacheBody("Basic", "C", 0, nil), want: http.StatusBadRequest},
		{name: "ok premium", path: "c1", body: redisCacheBody("Premium", "P", 1, nil), want: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRedisEnv(t)
			status, body, _ := e.do(t, http.MethodPut, redisBase+tt.path+redisAPI, tt.body)
			assert.Equal(t, tt.want, status, body)
		})
	}
}

func TestRedisNameTakenAcrossGroups(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	require.Equal(t, http.StatusCreated, status)

	other := strings.Replace(redisBase, "rg1", "rg2", 1) + "c1" + redisAPI
	status, body, _ := e.do(t, http.MethodPut, other, redisCacheBody("Basic", "C", 0, nil))
	assert.Equal(t, http.StatusConflict, status, body)
}

func TestRedisDataPlaneFailure(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	e.dp.failNext = true
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	assert.Equal(t, http.StatusBadRequest, status)

	status, _, _ = e.do(t, http.MethodGet, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, http.StatusNotFound, status, "a failed create stores nothing")
}

func TestRedisPatch(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	create := redisCacheBody("Standard", "C", 1, map[string]any{
		"redisConfiguration": map[string]any{"maxmemory-policy": "allkeys-lru", "notify-keyspace-events": "KEA"},
	})
	create["tags"] = map[string]any{"a": "1"}
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, create)
	require.Equal(t, http.StatusCreated, status)

	status, body, _ := e.do(t, http.MethodPatch, redisBase+"c1"+redisAPI, map[string]any{
		"properties": map[string]any{
			"enableNonSslPort":   true,
			"redisConfiguration": map[string]any{"maxmemory-policy": "noeviction"},
		},
	})
	require.Equal(t, http.StatusOK, status, body)

	p := propsOf(body)
	cfg, _ := p["redisConfiguration"].(map[string]any)
	assert.Equal(t, "noeviction", cfg["maxmemory-policy"])
	assert.Equal(t, "KEA", cfg["notify-keyspace-events"], "PATCH merges redisConfiguration")
	assert.Equal(t, true, p["enableNonSslPort"])
	assert.EqualValues(t, 6379, p["port"])
	assert.Equal(t, map[string]any{"a": "1"}, body["tags"], "omitted tags stay")
	assert.True(t, e.dp.specs[strings.ToLower(redisBase+"c1")].EnableNonSSLPort)

	status, _, _ = e.do(t, http.MethodPatch, redisBase+"missing"+redisAPI, map[string]any{})
	assert.Equal(t, http.StatusNotFound, status)
}

func TestRedisDataPlaneSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		name  string
		sku   string
		fam   string
		want  azurearm.RedisSpec
		cap   int
	}{
		{
			name: "defaults", sku: "Basic", fam: "C", cap: 0,
			want: azurearm.RedisSpec{SKU: "Basic", MinTLS: "1.2"},
		},
		{
			name: "premium rdb persistence", sku: "Premium", fam: "P", cap: 1,
			props: map[string]any{
				"minimumTlsVersion": "1.0",
				"redisConfiguration": map[string]any{
					"rdb-backup-enabled": "true", "authnotrequired": "true",
				},
			},
			want: azurearm.RedisSpec{SKU: "Premium", MinTLS: "1.0", Persistence: true, AuthNotRequired: true},
		},
		{
			name: "standard ignores persistence", sku: "Standard", fam: "C", cap: 1,
			props: map[string]any{"redisConfiguration": map[string]any{"rdb-backup-enabled": "true"}},
			want:  azurearm.RedisSpec{SKU: "Standard", MinTLS: "1.2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRedisEnv(t)
			status, body, _ := e.do(
				t,
				http.MethodPut,
				redisBase+"c1"+redisAPI,
				redisCacheBody(tt.sku, tt.fam, tt.cap, tt.props),
			)
			require.Equal(t, http.StatusCreated, status, body)
			assert.Equal(t, tt.want, e.dp.specs[strings.ToLower(redisBase+"c1")])
		})
	}
}

func TestRedisKeysAndActions(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Standard", "C", 1, nil))
	require.Equal(t, http.StatusCreated, status)

	status, keys, _ := e.do(t, http.MethodPost, redisBase+"c1/listKeys"+redisAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "pk", keys["primaryKey"])
	assert.Equal(t, "sk", keys["secondaryKey"])

	status, keys, _ = e.do(
		t,
		http.MethodPost,
		redisBase+"c1/regenerateKey"+redisAPI,
		map[string]any{"keyType": "Primary"},
	)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "pk2", keys["primaryKey"])
	assert.Equal(t, "sk", keys["secondaryKey"])

	status, msg, _ := e.do(
		t,
		http.MethodPost,
		redisBase+"c1/forceReboot"+redisAPI,
		map[string]any{"rebootType": "AllNodes"},
	)
	require.Equal(t, http.StatusOK, status)
	assert.NotEmpty(t, msg["message"])

	status, _, _ = e.do(t, http.MethodPost, redisBase+"c1/forceReboot"+redisAPI, map[string]any{})
	assert.Equal(t, http.StatusBadRequest, status, "rebootType is required")

	status, _, hdr := e.do(t, http.MethodPost, redisBase+"c1/import"+redisAPI, map[string]any{
		"format": "RDB", "files": []string{"https://a.blob.core.windows.net/c/b.rdb?sig=x"},
	})
	assert.Equal(t, http.StatusAccepted, status)
	assert.NotEmpty(t, hdr.Get("Azure-Asyncoperation"))

	status, _, _ = e.do(t, http.MethodPost, redisBase+"c1/export"+redisAPI, map[string]any{
		"format": "RDB", "prefix": "bk", "container": "https://a.blob.core.windows.net/c?sig=x",
	})
	assert.Equal(t, http.StatusAccepted, status)

	status, _, _ = e.do(t, http.MethodPost, redisBase+"c1/export"+redisAPI, map[string]any{"prefix": "bk"})
	assert.Equal(t, http.StatusBadRequest, status, "container is required")

	assert.Equal(t, []string{
		"reboot:AllNodes",
		"import:RDB:https://a.blob.core.windows.net/c/b.rdb?sig=x",
		"export:RDB:bk:https://a.blob.core.windows.net/c?sig=x",
	}, e.dp.calls)

	for _, action := range []string{"listKeys", "regenerateKey", "forceReboot", "import", "export"} {
		status, _, _ = e.do(t, http.MethodPost, redisBase+"missing/"+action+redisAPI, map[string]any{})
		assert.Equal(t, http.StatusNotFound, status, action)
	}
}

func TestRedisDelete(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
	require.Equal(t, http.StatusCreated, status)

	status, _, hdr := e.do(t, http.MethodDelete, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, http.StatusAccepted, status)
	assert.NotEmpty(t, hdr.Get("Azure-Asyncoperation"))
	assert.Equal(t, []string{"delete"}, e.dp.calls)

	status, _, _ = e.do(t, http.MethodGet, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)

	status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, http.StatusNoContent, status)
}

func TestRedisList(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)

	for _, n := range []string{"b", "a"} {
		status, _, _ := e.do(t, http.MethodPut, redisBase+n+redisAPI, redisCacheBody("Basic", "C", 0, nil))
		require.Equal(t, http.StatusCreated, status)
	}

	for _, path := range []string{
		"/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg1/providers/Microsoft.Cache/redis",
		"/subscriptions/00000000-0000-0000-0000-000000000000/providers/Microsoft.Cache/redis",
	} {
		status, body, _ := e.do(t, http.MethodGet, path+redisAPI, nil)
		require.Equal(t, http.StatusOK, status)

		vals, _ := body["value"].([]any)
		require.Len(t, vals, 2)
		assert.Equal(t, "a", vals[0].(map[string]any)["name"])
	}
}

func TestRedisFirewallRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		start string
		end   string
		want  int
	}{
		{name: "ok", start: "10.0.0.1", end: "10.0.0.9", want: http.StatusOK},
		{name: "single address", start: "10.0.0.1", end: "10.0.0.1", want: http.StatusOK},
		{name: "reversed", start: "10.0.0.9", end: "10.0.0.1", want: http.StatusBadRequest},
		{name: "ipv6", start: "::1", end: "::2", want: http.StatusBadRequest},
		{name: "garbage", start: "nope", end: "10.0.0.1", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRedisEnv(t)
			status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Basic", "C", 0, nil))
			require.Equal(t, http.StatusCreated, status)

			status, body, _ := e.do(t, http.MethodPut, redisBase+"c1/firewallRules/office"+redisAPI,
				map[string]any{"properties": map[string]any{"startIP": tt.start, "endIP": tt.end}})
			require.Equal(t, tt.want, status, body)

			if tt.want != http.StatusOK {
				return
			}

			assert.Equal(t, "Microsoft.Cache/Redis/firewallRules", body["type"])

			_, got, _ := e.do(t, http.MethodGet, redisBase+"c1/firewallRules/Office"+redisAPI, nil)
			assert.Equal(t, tt.start, propsOf(got)["startIP"])

			_, list, _ := e.do(t, http.MethodGet, redisBase+"c1/firewallRules"+redisAPI, nil)
			assert.Len(t, list["value"], 1)

			status, _, _ = e.do(t, http.MethodDelete, redisBase+"c1/firewallRules/office"+redisAPI, nil)
			assert.Equal(t, http.StatusOK, status)

			status, _, _ = e.do(t, http.MethodGet, redisBase+"c1/firewallRules/office"+redisAPI, nil)
			assert.Equal(t, http.StatusNotFound, status)
		})
	}

	t.Run("missing parent", func(t *testing.T) {
		t.Parallel()

		e := newRedisEnv(t)
		status, _, _ := e.do(t, http.MethodPut, redisBase+"nope/firewallRules/x"+redisAPI,
			map[string]any{"properties": map[string]any{"startIP": "10.0.0.1", "endIP": "10.0.0.1"}})
		assert.Equal(t, http.StatusNotFound, status)
	})
}

func TestRedisPatchSchedule(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Premium", "P", 1, nil))
	require.Equal(t, http.StatusCreated, status)

	path := redisBase + "c1/patchSchedules/default" + redisAPI
	status, _, _ = e.do(t, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, status, "no schedule yet")

	entries := []any{map[string]any{"dayOfWeek": "Monday", "startHourUtc": float64(3), "maintenanceWindow": "PT5H"}}
	status, body, _ := e.do(
		t,
		http.MethodPut,
		path,
		map[string]any{"properties": map[string]any{"scheduleEntries": entries}},
	)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, entries, propsOf(body)["scheduleEntries"])

	status, _, _ = e.do(t, http.MethodPut, redisBase+"c1/patchSchedules/other"+redisAPI,
		map[string]any{"properties": map[string]any{"scheduleEntries": entries}})
	assert.Equal(t, http.StatusBadRequest, status, "only default exists")

	for _, bad := range []any{
		[]any{},
		[]any{map[string]any{"dayOfWeek": "Funday", "startHourUtc": 1}},
		[]any{map[string]any{"dayOfWeek": "Monday", "startHourUtc": 24}},
	} {
		status, _, _ = e.do(
			t,
			http.MethodPut,
			path,
			map[string]any{"properties": map[string]any{"scheduleEntries": bad}},
		)
		assert.Equal(t, http.StatusBadRequest, status)
	}

	_, list, _ := e.do(t, http.MethodGet, redisBase+"c1/patchSchedules"+redisAPI, nil)
	assert.Len(t, list["value"], 1)

	status, _, _ = e.do(t, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusOK, status)

	status, _, _ = e.do(t, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestRedisLinkedServers(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)

	for _, n := range []string{"a", "b"} {
		status, _, _ := e.do(t, http.MethodPut, redisBase+n+redisAPI, redisCacheBody("Premium", "P", 1, nil))
		require.Equal(t, http.StatusCreated, status)
	}

	status, _, _ := e.do(t, http.MethodPut, redisBase+"std"+redisAPI, redisCacheBody("Standard", "C", 1, nil))
	require.Equal(t, http.StatusCreated, status)

	link := func(name string, props map[string]any) (int, map[string]any) {
		s, b, _ := e.do(
			t,
			http.MethodPut,
			redisBase+"a/linkedServers/"+name+redisAPI,
			map[string]any{"properties": props},
		)

		return s, b
	}

	good := map[string]any{
		"linkedRedisCacheId": redisBase + "b", "linkedRedisCacheLocation": "local", "serverRole": "Secondary",
	}

	status, body := link("b", good)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "Succeeded", propsOf(body)["provisioningState"])
	assert.Equal(t, "a.redis.cache.windows.net", propsOf(body)["primaryHostName"])

	status, _ = link("c", good)
	assert.Equal(t, http.StatusBadRequest, status, "name must match the linked cache")

	status, _ = link("b", map[string]any{
		"linkedRedisCacheId": redisBase + "b", "linkedRedisCacheLocation": "local", "serverRole": "Boss",
	})
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = link("std", map[string]any{
		"linkedRedisCacheId": redisBase + "std", "linkedRedisCacheLocation": "local", "serverRole": "Secondary",
	})
	assert.Equal(t, http.StatusBadRequest, status, "linked cache must be Premium")

	_, parent, _ := e.do(t, http.MethodGet, redisBase+"a"+redisAPI, nil)
	assert.Len(t, propsOf(parent)["linkedServers"], 1)

	_, list, _ := e.do(t, http.MethodGet, redisBase+"a/linkedServers"+redisAPI, nil)
	assert.Len(t, list["value"], 1)

	status, _, _ = e.do(t, http.MethodDelete, redisBase+"a/linkedServers/b"+redisAPI, nil)
	assert.Equal(t, http.StatusOK, status)
}

func TestRedisSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, _, _ := e.do(t, http.MethodPut, redisBase+"c1"+redisAPI, redisCacheBody("Premium", "P", 1, nil))
	require.Equal(t, http.StatusCreated, status)

	status, _, _ = e.do(t, http.MethodPut, redisBase+"c1/firewallRules/r"+redisAPI,
		map[string]any{"properties": map[string]any{"startIP": "10.0.0.1", "endIP": "10.0.0.2"}})
	require.Equal(t, http.StatusOK, status)

	status, _, _ = e.do(t, http.MethodPut, redisBase+"c1/patchSchedules/default"+redisAPI, map[string]any{
		"properties": map[string]any{
			"scheduleEntries": []any{map[string]any{"dayOfWeek": "Friday", "startHourUtc": 1}},
		},
	})
	require.Equal(t, http.StatusOK, status)

	snap := azurearm.RedisSnapshotJSON(e.p)

	other := newRedisEnv(t)
	other.dp.specs = e.dp.specs
	azurearm.RedisRestoreJSON(t, other.p, snap)

	for _, path := range []string{"", "/firewallRules/r", "/patchSchedules/default"} {
		status, _, _ = other.do(t, http.MethodGet, redisBase+"c1"+path+redisAPI, nil)
		assert.Equal(t, http.StatusOK, status, path)
	}

	other.p.Reset()

	status, _, _ = other.do(t, http.MethodGet, redisBase+"c1"+redisAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestRedisProviderRegistration(t *testing.T) {
	t.Parallel()

	e := newRedisEnv(t)
	status, body, _ := e.do(t, http.MethodGet,
		"/subscriptions/00000000-0000-0000-0000-000000000000/providers/Microsoft.Cache"+redisAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "Microsoft.Cache", body["namespace"])
}
