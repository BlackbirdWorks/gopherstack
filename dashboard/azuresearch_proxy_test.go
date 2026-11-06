package dashboard_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

const (
	srchAPI     = "?api-version=2025-05-01"
	srchRoot    = "/dashboard/api/v1/azure/search"
	srchKeys    = "/dashboard/api/v1/azure/searchkeys"
	srchData    = "/dashboard/api/v1/azure/searchdata"
	srchIDs     = "/" + armSub + "/demo/hotels"
	srchService = armBase + "Microsoft.Search/searchServices/hotels"
	srchDataAPI = "api-version=2024-07-01"
	srchSecret  = "Server=db;Password=hunter2"
)

var errSrchOffline = errors.New("storage offline")

// srchPlane adapts the data plane backend to the Microsoft.Search ARM provider.
type srchPlane struct{ b *azuresearch.InMemoryBackend }

func (p srchPlane) Port() int { return azuresearch.DefaultPort }

func (p srchPlane) PutService(name string, cfg azurearm.SearchServiceConfig) (time.Time, error) {
	return p.b.PutService(name, azuresearch.ServiceConfig(cfg))
}

func (p srchPlane) DeleteService(name string) error { return p.b.DeleteService(name) }

func (p srchPlane) AdminKeys(name string) (azurearm.SearchAdminKeys, error) {
	k, err := p.b.Keys(name)

	return azurearm.SearchAdminKeys{Primary: k.Primary, Secondary: k.Secondary}, err
}

func (p srchPlane) RegenerateAdminKey(name, kind string) (azurearm.SearchAdminKeys, error) {
	k, err := p.b.RegenerateAdminKey(name, kind)

	return azurearm.SearchAdminKeys{Primary: k.Primary, Secondary: k.Secondary}, err
}

func (p srchPlane) QueryKeys(name string) ([]azurearm.SearchQueryKey, error) {
	keys, err := p.b.ListQueryKeys(name)
	out := make([]azurearm.SearchQueryKey, 0, len(keys))

	for _, k := range keys {
		out = append(out, azurearm.SearchQueryKey(k))
	}

	return out, err
}

func (p srchPlane) CreateQueryKey(name, keyName string) (azurearm.SearchQueryKey, error) {
	k, err := p.b.CreateQueryKey(name, keyName)

	return azurearm.SearchQueryKey(k), err
}

func (p srchPlane) DeleteQueryKey(name, key string) error { return p.b.DeleteQueryKey(name, key) }

type srchBlobs struct{}

func (srchBlobs) ListBlobs(container string) ([]azuresearch.BlobEntry, error) {
	if container != "docs" {
		return nil, errSrchOffline
	}

	return []azuresearch.BlobEntry{{Name: "a.txt", ETag: "e1", Size: 5}}, nil
}

func (srchBlobs) ReadBlob(string, string) ([]byte, error) { return []byte("hello"), nil }

type srchFixture struct {
	h     *dashboard.DashboardHandler
	b     *azuresearch.InMemoryBackend
	admin string
}

func newSearchFixture(t *testing.T) srchFixture {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := svc.(*azurearm.Handler)
	require.True(t, ok)
	t.Cleanup(func() { armH.Shutdown(t.Context()) })

	b := azuresearch.NewInMemoryBackend()
	b.SetBlobSource(srchBlobs{})
	armH.Registry.SetSearch(srchPlane{b: b})

	sh := azuresearch.NewHandler(b, azuresearch.DefaultSettings())
	h := dashboard.NewHandler(dashboard.Config{AzureARMOps: armH, AzureSearchOps: sh})

	for _, s := range []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"westus"}`},
		{srchService + srchAPI, `{"location":"westus","sku":{"name":"standard"},` +
			`"properties":{"replicaCount":2,"partitionCount":1}}`},
	} {
		rec := lbDo(t, h, http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, s.body)
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", s.path, rec.Body.String())
	}

	keys, err := b.Keys("hotels")
	require.NoError(t, err)

	f := srchFixture{h: h, b: b, admin: keys.Primary}
	f.seed(t)

	return f
}

// seed fills the service through the data plane handler, the way an SDK client would.
func (f srchFixture) seed(t *testing.T) {
	t.Helper()

	e := echo.New()
	e.Any("/*", azuresearch.NewHandler(f.b, azuresearch.DefaultSettings()).Handler())

	send := func(method, path, body string, want int) {
		req := httptest.NewRequest(method, path+"?"+srchDataAPI, strings.NewReader(body))
		req.Host = "hotels.search.windows.net"
		req.Header.Set("Api-Key", f.admin)
		req.Header.Set("Content-Type", "application/json")

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, "%s %s: %s", method, path, rec.Body.String())
	}

	send(http.MethodPost, "/indexes", `{"name":"hotels","fields":[`+
		`{"name":"id","type":"Edm.String","key":true},`+
		`{"name":"name","type":"Edm.String","searchable":true},`+
		`{"name":"category","type":"Edm.String","filterable":true,"facetable":true},`+
		`{"name":"rating","type":"Edm.Double","filterable":true,"sortable":true}]}`, http.StatusCreated)
	send(http.MethodPost, "/indexes/hotels/docs/search.index", `{"value":[`+
		`{"@search.action":"upload","id":"1","name":"Grand Hotel","category":"hotel","rating":4.5},`+
		`{"@search.action":"upload","id":"2","name":"Budget Inn","category":"motel","rating":3.0},`+
		`{"@search.action":"upload","id":"3","name":"Royal Hotel","category":"hotel","rating":4.8}]}`, http.StatusOK)
	send(http.MethodPut, "/datasources/ds", `{"name":"ds","type":"azureblob","credentials":`+
		`{"connectionString":"`+srchSecret+`"},"container":{"name":"docs"}}`, http.StatusCreated)
	send(http.MethodPut, "/indexers/ix", `{"name":"ix","dataSourceName":"ds","targetIndexName":"hotels",`+
		`"fieldMappings":[{"sourceFieldName":"metadata_storage_path","targetFieldName":"id",`+
		`"mappingFunction":{"name":"base64Encode"}}]}`, http.StatusCreated)
}

func srchGet(t *testing.T, h *dashboard.DashboardHandler, path string) map[string]any {
	t.Helper()

	rec := lbDo(t, h, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, rec.Code, "%s: %s", path, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func TestAzureSearchProxyServices(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)

	list := srchGet(t, f.h, srchRoot+armBase+"Microsoft.Search/searchServices"+srchAPI)
	items, _ := list["value"].([]any)
	require.Len(t, items, 1)

	one := srchGet(t, f.h, srchRoot+srchService+srchAPI)
	props, _ := one["properties"].(map[string]any)
	assert.EqualValues(t, 2, props["replicaCount"])
	assert.EqualValues(t, 1, props["partitionCount"])
	assert.Contains(t, props["endpoint"], "hotels")
}

func TestAzureSearchProxyServicesAllowList(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{
			"admin keys over get",
			http.MethodGet,
			srchRoot + srchService + "/listAdminKeys" + srchAPI,
			http.StatusBadRequest,
		},
		{
			"query keys over get",
			http.MethodGet,
			srchRoot + srchService + "/listQueryKeys" + srchAPI,
			http.StatusBadRequest,
		},
		{
			"other provider",
			http.MethodGet,
			srchRoot + armBase + "Microsoft.Logic/workflows" + srchAPI,
			http.StatusBadRequest,
		},
		{
			"resource groups",
			http.MethodGet,
			srchRoot + "/subscriptions/" + armSub + "/resourceGroups" + srchAPI,
			http.StatusBadRequest,
		},
		{"traversal", http.MethodGet, srchRoot + "/subscriptions/" + armSub + "/../x" + srchAPI, http.StatusBadRequest},
		{"put", http.MethodPut, srchRoot + srchService + srchAPI, http.StatusNotFound},
		{"delete", http.MethodDelete, srchRoot + srchService + srchAPI, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, tt.method, tt.path, `{}`)
			assert.Contains(t, []int{tt.want, http.StatusMethodNotAllowed}, rec.Code, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), f.admin)
		})
	}
}

func TestAzureSearchKeysAreMaskedUntilRevealed(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	keys, err := f.b.Keys("hotels")
	require.NoError(t, err)

	query, err := f.b.ListQueryKeys("hotels")
	require.NoError(t, err)
	require.NotEmpty(t, query)

	list := lbDo(t, f.h, http.MethodGet, srchKeys+srchIDs, "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	assert.Equal(t, "no-store", list.Header().Get("Cache-Control"))

	for _, secret := range []string{keys.Primary, keys.Secondary, query[0].Key} {
		assert.NotContains(t, list.Body.String(), secret)
	}

	type masked struct {
		Name   string `json:"name"`
		Fp     string `json:"fp"`
		Masked string `json:"masked"`
	}

	var shown struct {
		Admin struct {
			Primary   string `json:"primary"`
			Secondary string `json:"secondary"`
		} `json:"admin"`
		Query []masked `json:"query"`
	}

	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &shown))
	assert.True(t, strings.HasSuffix(keys.Primary, strings.TrimPrefix(shown.Admin.Primary, "••••••••")))
	assert.NotEqual(t, keys.Primary, shown.Admin.Primary)
	require.NotEmpty(t, shown.Query)
	assert.Len(t, shown.Query[0].Fp, 8)

	reveal := lbDo(t, f.h, http.MethodPost, srchKeys+srchIDs+"/admin/reveal", "")
	require.Equal(t, http.StatusOK, reveal.Code)
	assert.Equal(t, "no-store", reveal.Header().Get("Cache-Control"))
	assert.JSONEq(t, `{"primaryKey":"`+keys.Primary+`","secondaryKey":"`+keys.Secondary+`"}`, reveal.Body.String())

	one := lbDo(t, f.h, http.MethodPost, srchKeys+srchIDs+"/query/"+shown.Query[0].Fp+"/reveal", "")
	require.Equal(t, http.StatusOK, one.Code, one.Body.String())
	assert.Contains(t, one.Body.String(), query[0].Key)
}

func TestAzureSearchQueryKeyLifecycle(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)

	made := lbDo(t, f.h, http.MethodPost, srchKeys+srchIDs+"/query", `{"name":"web front"}`)
	require.Equal(t, http.StatusCreated, made.Code, made.Body.String())

	var created struct {
		Name string `json:"name"`
		Fp   string `json:"fp"`
	}

	require.NoError(t, json.Unmarshal(made.Body.Bytes(), &created))
	assert.Equal(t, "web front", created.Name)

	all, err := f.b.ListQueryKeys("hotels")
	require.NoError(t, err)

	for _, k := range all {
		assert.NotContains(t, made.Body.String(), k.Key, "creating a key does not echo the value")
	}

	del := lbDo(t, f.h, http.MethodDelete, srchKeys+srchIDs+"/query/"+created.Fp, "")
	require.Equal(t, http.StatusNoContent, del.Code, del.Body.String())

	after, err := f.b.ListQueryKeys("hotels")
	require.NoError(t, err)
	assert.Len(t, after, len(all)-1)

	gone := lbDo(t, f.h, http.MethodDelete, srchKeys+srchIDs+"/query/"+created.Fp, "")
	assert.Equal(t, http.StatusNotFound, gone.Code)
}

func TestAzureSearchKeysRejectBadInput(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"bad name", http.MethodPost, srchKeys + srchIDs + "/query", `{"name":"a/b"}`, http.StatusBadRequest},
		{
			"extra field",
			http.MethodPost,
			srchKeys + srchIDs + "/query",
			`{"name":"ok","key":"mine"}`,
			http.StatusBadRequest,
		},
		{"empty name", http.MethodPost, srchKeys + srchIDs + "/query", `{}`, http.StatusBadRequest},
		{"bad fingerprint", http.MethodDelete, srchKeys + srchIDs + "/query/NOTHEX", "", http.StatusBadRequest},
		{
			"unknown fingerprint",
			http.MethodPost,
			srchKeys + srchIDs + "/query/00000000/reveal",
			"",
			http.StatusNotFound,
		},
		{"unknown service", http.MethodGet, srchKeys + "/" + armSub + "/demo/ghost", "", http.StatusNotFound},
		{
			"unknown group",
			http.MethodPost,
			srchKeys + "/" + armSub + "/other/hotels/admin/reveal",
			"",
			http.StatusNotFound,
		},
		{"bad subscription", http.MethodGet, srchKeys + "/a%20b/demo/hotels", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), f.admin)
		})
	}
}

func TestAzureSearchDataPlaneReads(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)

	idx := srchGet(t, f.h, srchData+srchIDs+"/indexes")
	list, _ := idx["value"].([]any)
	require.Len(t, list, 1)

	stats := srchGet(t, f.h, srchData+srchIDs+"/indexes/hotels/search.stats")
	assert.EqualValues(t, 3, stats["documentCount"])

	svc := srchGet(t, f.h, srchData+srchIDs+"/servicestats")
	assert.Contains(t, svc, "counters")

	paths := []string{
		"/indexes/hotels", "/indexers", "/indexers/ix", "/indexers/ix/search.status", "/datasources", "/skillsets",
		"/synonymmaps",
	}

	for _, p := range paths {
		srchGet(t, f.h, srchData+srchIDs+p)
	}

	body := lbDo(t, f.h, http.MethodGet, srchData+srchIDs+"/datasources", "").Body.String()
	assert.NotContains(t, body, "hunter2", "connection strings never reach the browser")
	assert.NotContains(t, body, f.admin)
}

func TestAzureSearchExplorerFilterAndFacets(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	path := srchData + srchIDs + "/indexes/hotels/docs/search.post.search"

	rec := lbDo(t, f.h, http.MethodPost, path, `{"search":"hotel","filter":"rating ge 4","orderby":"rating desc",`+
		`"select":"id,name","facets":["category"],"top":5,"count":true}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	type facet struct {
		Value string `json:"value"`
		Count int    `json:"count"`
	}

	var out struct {
		Facets map[string][]facet `json:"@search.facets"`
		Value  []map[string]any   `json:"value"`
		Count  int                `json:"@odata.count"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, 2, out.Count)
	require.Len(t, out.Value, 2)
	assert.Equal(t, "Royal Hotel", out.Value[0]["name"], "ordered by rating descending")
	require.Len(t, out.Facets["category"], 1)
	assert.Equal(t, "hotel", out.Facets["category"][0].Value)
	assert.NotContains(t, rec.Body.String(), f.admin)
}

func TestAzureSearchExplorerRebuildsTheBody(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	path := srchData + srchIDs + "/indexes/hotels/docs/search.post.search"

	tests := []struct {
		name string
		body string
		want int
	}{
		{"minimal", `{"search":"*"}`, http.StatusOK},
		{"empty", `{}`, http.StatusOK},
		{"unknown field", `{"search":"*","vectorQueries":[{"kind":"vector"}]}`, http.StatusBadRequest},
		{"scoring parameters", `{"search":"*","scoringParameters":["a-1"]}`, http.StatusBadRequest},
		{"top too large", `{"search":"*","top":1001}`, http.StatusBadRequest},
		{"negative skip", `{"search":"*","skip":-1}`, http.StatusBadRequest},
		{"bad query type", `{"search":"*","queryType":"semantic"}`, http.StatusBadRequest},
		{"bad search mode", `{"search":"*","searchMode":"some"}`, http.StatusBadRequest},
		{
			"too many facets",
			`{"search":"*","facets":[` + strings.Repeat(`"category",`, 40) + `"x"]}`,
			http.StatusBadRequest,
		},
		{"oversize text", `{"search":"` + strings.Repeat("a", 5000) + `"}`, http.StatusBadRequest},
		{"not json", `search=*`, http.StatusBadRequest},
		{"wrong types", `{"search":5}`, http.StatusBadRequest},
		{"bad filter reaches the service", `{"search":"*","filter":"nope nope"}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
		})
	}
}

func TestAzureSearchDataPlaneAllowList(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	base := srchData + srchIDs

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"create index", http.MethodPost, base + "/indexes"},
		{"put index", http.MethodPut, base + "/indexes/hotels"},
		{"delete index", http.MethodDelete, base + "/indexes/hotels"},
		{"upload documents", http.MethodPost, base + "/indexes/hotels/docs/search.index"},
		{"read documents", http.MethodGet, base + "/indexes/hotels/docs"},
		{"document by key", http.MethodGet, base + "/indexes/hotels/docs/1"},
		{"count", http.MethodGet, base + "/indexes/hotels/docs/$count"},
		{"analyze", http.MethodPost, base + "/indexes/hotels/search.analyze"},
		{"put datasource", http.MethodPut, base + "/datasources/ds"},
		{"delete indexer", http.MethodDelete, base + "/indexers/ix"},
		{"odata key syntax", http.MethodGet, base + "/indexes('hotels')"},
		{"traversal", http.MethodGet, base + "/indexes/../servicestats"},
		{"empty", http.MethodGet, base + "/"},
		{"unknown root", http.MethodGet, base + "/admin"},
		{"search over get", http.MethodGet, base + "/indexes/hotels/docs/search.post.search"},
		{"run over get", http.MethodGet, base + "/indexers/ix/search.run"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, tt.method, tt.path, `{"name":"x"}`)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest, rec.Body.String())
			assert.NotEqual(t, http.StatusOK, rec.Code)
		})
	}

	still := srchGet(t, f.h, base+"/indexes")
	list, _ := still["value"].([]any)
	assert.Len(t, list, 1, "nothing was created or removed")
}

func TestAzureSearchIndexerRunAndReset(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	base := srchData + srchIDs + "/indexers/ix"

	before := srchGet(t, f.h, base+"/search.status")
	assert.Equal(t, "unknown", before["status"])

	run := lbDo(t, f.h, http.MethodPost, base+"/search.run", "")
	require.Equal(t, http.StatusAccepted, run.Code, run.Body.String())

	after := srchGet(t, f.h, base+"/search.status")
	last, _ := after["lastResult"].(map[string]any)
	require.NotNil(t, last)
	assert.Equal(t, "success", last["status"], "%v", last)

	reset := lbDo(t, f.h, http.MethodPost, base+"/search.reset", "")
	assert.Equal(t, http.StatusNoContent, reset.Code, reset.Body.String())
}

func TestAzureSearchAbsentBackendsAnswer503(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, tt := range []struct{ name, method, path string }{
		{"arm", http.MethodGet, srchRoot + armBase + "Microsoft.Search/searchServices" + srchAPI},
		{"keys", http.MethodGet, srchKeys + srchIDs},
		{"reveal", http.MethodPost, srchKeys + srchIDs + "/admin/reveal"},
		{"data", http.MethodGet, srchData + srchIDs + "/indexes"},
		{"query", http.MethodPost, srchData + srchIDs + "/indexes/hotels/docs/search.post.search"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, h, tt.method, tt.path, `{}`)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		})
	}
}

func TestAzureSearchNoSecretInAnyRead(t *testing.T) {
	t.Parallel()

	f := newSearchFixture(t)
	keys, err := f.b.Keys("hotels")
	require.NoError(t, err)

	query, err := f.b.ListQueryKeys("hotels")
	require.NoError(t, err)

	secrets := make([]string, 0, 3+len(query))
	secrets = append(secrets, keys.Primary, keys.Secondary, "hunter2")
	for _, q := range query {
		secrets = append(secrets, q.Key)
	}

	paths := []string{
		srchRoot + srchService + srchAPI,
		srchRoot + armBase + "Microsoft.Search/searchServices" + srchAPI,
		srchKeys + srchIDs,
		srchData + srchIDs + "/indexes",
		srchData + srchIDs + "/indexers",
		srchData + srchIDs + "/indexers/ix",
		srchData + srchIDs + "/datasources",
		srchData + srchIDs + "/datasources/ds",
		srchData + srchIDs + "/skillsets",
		srchData + srchIDs + "/servicestats",
	}

	for _, p := range paths {
		rec := lbDo(t, f.h, http.MethodGet, p, "")
		for _, s := range secrets {
			assert.NotContains(t, rec.Body.String(), s, p)
		}
	}
}
