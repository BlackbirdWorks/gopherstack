package azurearm_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingBackend struct {
	srv  *httptest.Server
	hits atomic.Int64
}

func newCountingBackend(t *testing.T) *countingBackend {
	t.Helper()

	cb := &countingBackend{}
	cb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := cb.hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)

		w.Header().Set("X-Hit", strconv.FormatInt(n, 10))

		if strings.HasSuffix(r.URL.Path, "/404") {
			w.WriteHeader(http.StatusNotFound)
		}

		_, _ = io.WriteString(w, "hit="+strconv.FormatInt(n, 10))
	}))
	t.Cleanup(cb.srv.Close)

	return cb
}

func (e *gwEnv) cacheAPI(t *testing.T) *countingBackend {
	t.Helper()

	cb := newCountingBackend(t)
	e.putAPI(t, "data", "data", cb.srv.URL)
	e.putOp(t, "data", "get", "get", "/{id}")
	e.putOp(t, "data", "post", "post", "/{id}")

	return cb
}

func TestAPIMGateway_CacheLookupStore(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	cb := e.cacheAPI(t)

	e.policy(
		t,
		"/apis/data",
		`<policies><inbound><base /><cache-lookup vary-by-developer="false" vary-by-developer-groups="false">
		<vary-by-query-parameter>v</vary-by-query-parameter><vary-by-header>Accept</vary-by-header></cache-lookup></inbound>
		<backend><base /></backend><outbound><base /><cache-store duration="60" /></outbound></policies>`,
		http.StatusCreated,
	)

	get := func(path string, hdr map[string]string) (int, string, http.Header) {
		return e.call(t, "GET", path, "", hdr)
	}

	_, body, _ := get("/data/a?v=1", nil)
	assert.Equal(t, "hit=1", body)

	_, body, hdr := get("/data/a?v=1&ignored=x", nil)
	assert.Equal(t, "hit=1", body, "a non-varied query parameter shares the entry")
	assert.Equal(t, "1", hdr.Get("X-Hit"))

	_, body, _ = get("/data/a?v=2", nil)
	assert.Equal(t, "hit=2", body, "vary-by-query-parameter")

	_, body, _ = get("/data/a?v=1", map[string]string{"Accept": "text/csv"})
	assert.Equal(t, "hit=3", body, "vary-by-header")

	_, body, _ = e.call(t, "POST", "/data/a", "", nil)
	assert.Equal(t, "hit=4", body)

	_, body, _ = e.call(t, "POST", "/data/a", "", nil)
	assert.Equal(t, "hit=5", body, "only GET responses are cached")

	e.clock.Advance(61 * time.Second)

	_, body, _ = get("/data/a?v=1", nil)
	assert.Equal(t, "hit=6", body, "the entry expired on the injected clock")
	assert.Equal(t, int64(6), cb.hits.Load())
}

func TestAPIMGateway_CacheAuthorizedAndStatus(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name      string
		storeAttr string
		lookup    string
		path      string
		hdr       map[string]string
		wantHits  int64
	}{
		{"authorized not cached", "", "", "/data/a", map[string]string{"Authorization": "Bearer x"}, 2},
		{
			"private allowed",
			"",
			` allow-private-response-caching="true"`,
			"/data/a",
			map[string]string{"Authorization": "Bearer x"},
			1,
		},
		{
			"private expression",
			"",
			` allow-private-response-caching="@(true)"`,
			"/data/a",
			map[string]string{"Authorization": "Bearer x"},
			1,
		},
		{"404 not cached by default", "", "", "/data/404", nil, 2},
		{"404 cached with cache-response", ` cache-response="true"`, "", "/data/404", nil, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			cb := e.cacheAPI(t)

			e.policy(
				t,
				"/apis/data",
				`<policies><inbound><base /><cache-lookup vary-by-developer="false" vary-by-developer-groups="false"`+
					tc.lookup+`><vary-by-header>Authorization</vary-by-header></cache-lookup></inbound><backend><base /></backend>
				<outbound><base /><cache-store duration="60"`+tc.storeAttr+` /></outbound></policies>`,
				http.StatusCreated,
			)

			e.call(t, "GET", tc.path, "", tc.hdr)
			e.call(t, "GET", tc.path, "", tc.hdr)
			assert.Equal(t, tc.wantHits, cb.hits.Load())
		})
	}
}

func TestAPIMGateway_CacheDownstreamAndDeveloper(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	cb := e.cacheAPI(t)

	e.policy(
		t,
		"/apis/data",
		`<policies><inbound><base /><cache-lookup vary-by-developer="true" vary-by-developer-groups="false"
		downstream-caching-type="public" must-revalidate="false" /></inbound><backend><base /></backend>
		<outbound><base /><cache-store duration="60" /></outbound></policies>`,
		http.StatusCreated,
	)

	mk := func(id, owner string) string {
		_, err := e.f.NewSubscriptionClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, id,
			armapimanagement.SubscriptionCreateParameters{
				Properties: &armapimanagement.SubscriptionCreateParameterProperties{
					DisplayName: new(id), Scope: new("/apis"), OwnerID: new("/users/" + owner),
					State: new(armapimanagement.SubscriptionStateActive),
				},
			}, nil)
		require.NoError(t, err)

		sec, err := e.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, id, nil)
		require.NoError(t, err)

		return *sec.PrimaryKey
	}

	k1, k2 := mk("d1", "u1"), mk("d2", "u2")

	call := func(k string) (string, http.Header) {
		_, body, hdr := e.call(t, "GET", "/data/a", "", map[string]string{"Ocp-Apim-Subscription-Key": k})

		return body, hdr
	}

	body, hdr := call(k1)
	assert.Equal(t, "hit=1", body)
	assert.Equal(t, "public, max-age=60", hdr.Get("Cache-Control"))

	body, hdr = call(k1)
	assert.Equal(t, "hit=1", body)
	assert.Equal(t, "public, max-age=60", hdr.Get("Cache-Control"))

	body, _ = call(k2)
	assert.Equal(t, "hit=2", body, "vary-by-developer separates owners")
	assert.Equal(t, int64(2), cb.hits.Load())
}

func TestAPIMGateway_CacheValue(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`
		<cache-lookup-value key="k1" variable-name="cv" default-value="none" />`+
		headerPolicy("X-Cv", `@((string)context.Variables["cv"])`)+`
		<choose><when condition="@(context.Request.Url.Query.ContainsKey(&quot;drop&quot;))">
			<cache-remove-value key="k1" /></when></choose>
		<cache-store-value key="k1" duration="60"
			value="@(&quot;stored-&quot; + context.Request.Url.Query.GetValueOrDefault(&quot;v&quot;, &quot;x&quot;))" />`,
		"", ""), http.StatusCreated)

	get := func(q string) string {
		_, body, _ := e.call(t, "GET", "/echo/items/1"+q, "", nil)

		return decodeEcho(t, body).Header["X-Cv"][0]
	}

	assert.Equal(t, "none", get("?v=a"))
	assert.Equal(t, "stored-a", get("?v=b"))
	assert.Equal(t, "stored-b", get(""))

	e.clock.Advance(61 * time.Second)
	assert.Equal(t, "none", get("?v=c"), "the value expired")
	assert.Equal(t, "stored-c", get("?drop=1"), "lookup runs before the removal")
}

func TestAPIMCache_Validation(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	tests := []struct{ name, in, out, want string }{
		{"store in inbound", `<cache-store duration="1" />`, "", "not allowed in the inbound"},
		{"lookup in outbound", "", `<cache-lookup />`, "not allowed in the outbound"},
		{"store no duration", "", `<cache-store />`, "'duration' is required"},
		{"bad downstream", `<cache-lookup downstream-caching-type="world" />`, "", "downstream-caching-type"},
		{"bad caching type", `<cache-lookup caching-type="disk" />`, "", "caching-type"},
		{"lookup-value no variable", `<cache-lookup-value key="k" />`, "", "'variable-name' is required"},
		{"bad bool", `<cache-lookup must-revalidate="yes" />`, "", "must-revalidate"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := e.policy(t, "/apis/echo", policyDoc(tc.in, "", tc.out), http.StatusBadRequest)
			assert.Contains(t, body, tc.want)
		})
	}
}
