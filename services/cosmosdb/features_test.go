package cosmosdb_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestRU_ChargeScalesWithPayloadAndOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)
	createTestContainer(t, h)

	pkHdr := map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`}
	charge := func(rec interface{ Header() http.Header }) float64 {
		v, err := strconv.ParseFloat(rec.Header().Get("X-Ms-Request-Charge"), 64)
		require.NoError(t, err)

		return v
	}

	small := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, []byte(`{"id":"s","pk":"a"}`))
	require.Equal(t, http.StatusCreated, small.Code)

	big := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil,
		fmt.Appendf(nil, `{"id":"b","pk":"a","blob":%q}`, string(make([]byte, 0))+fmt.Sprintf("%0*d", 20000, 0)))
	require.Equal(t, http.StatusCreated, big.Code)

	assert.InDelta(t, 5.5+2*0.3, charge(small), 0.01, "small create: 5.5 RU plus 0.3 for each of its two indexed paths")
	assert.Greater(t, charge(big), charge(small)+5, "larger payload costs more")

	readSmall := doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/s", pkHdr, nil)
	readBig := doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/b", pkHdr, nil)

	assert.InDelta(t, 1.0, charge(readSmall), 0.01)
	assert.Greater(t, charge(readBig), charge(readSmall))
	assert.Less(t, charge(readSmall), charge(small), "reads are cheaper than writes")

	replace := doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll/docs/s", pkHdr, []byte(`{"id":"s","pk":"a"}`))
	require.Equal(t, http.StatusOK, replace.Code)
	assert.InDelta(t, 2*charge(small), charge(replace), 0.02, "replace costs twice a create")

	del := doRequest(t, h, http.MethodDelete, "/dbs/mydb/colls/mycoll/docs/s", pkHdr, nil)
	assert.InDelta(t, 5.5, charge(del), 0.01)
}

func TestRU_QueryChargeGrowsWithScannedItems(t *testing.T) {
	t.Parallel()

	queryHdr := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}

	run := func(n int) float64 {
		h := newTestHandler(t)
		createTestDatabase(t, h)
		createTestContainer(t, h)

		for i := range n {
			rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil,
				fmt.Appendf(nil, `{"id":"d%d","pk":"a"}`, i))
			require.Equal(t, http.StatusCreated, rec.Code)
		}

		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", queryHdr,
			[]byte(`{"query":"SELECT VALUE COUNT(1) FROM c"}`))
		require.Equal(t, http.StatusOK, rec.Code)

		v, err := strconv.ParseFloat(rec.Header().Get("X-Ms-Request-Charge"), 64)
		require.NoError(t, err)

		return v
	}

	few, many := run(1), run(400)

	assert.GreaterOrEqual(t, few, 1.4)
	assert.Greater(t, many, few)
}

func TestHierarchicalPartitionKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil,
		[]byte(`{"id":"mh","partitionKey":{"paths":["/tenant","/user","/session"],"kind":"MultiHash","version":2}}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	pk := decodeBody(t, rec)["partitionKey"].(map[string]any)
	assert.Equal(t, "MultiHash", pk["kind"])
	assert.Len(t, pk["paths"], 3)

	docs := []string{
		`{"id":"1","tenant":"t1","user":"u1","session":"s1"}`,
		`{"id":"1","tenant":"t1","user":"u1","session":"s2"}`,
		`{"id":"1","tenant":"t1","user":"u2","session":"s1"}`,
		`{"id":"1","tenant":"t2","user":"u1","session":"s1"}`,
	}

	for _, d := range docs {
		rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mh/docs", nil, []byte(d))
		require.Equal(t, http.StatusCreated, rec.Code, "same id under different keys is allowed: %s", d)
	}

	rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mh/docs", nil, []byte(docs[0]))
	assert.Equal(t, http.StatusConflict, rec.Code)

	get := func(header string) int {
		return doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mh/docs/1",
			map[string]string{"X-Ms-Documentdb-Partitionkey": header}, nil).Code
	}

	assert.Equal(t, http.StatusOK, get(`["t1","u1","s2"]`))
	assert.Equal(t, http.StatusNotFound, get(`["t1","u2","s2"]`))
	assert.Equal(t, http.StatusBadRequest, get(`["t1","u1"]`), "point operations need every component")
	assert.Equal(t, http.StatusBadRequest, get(`["t1"]`))

	queryHdr := func(pkHeader string) map[string]string {
		m := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}
		if pkHeader != "" {
			m["X-Ms-Documentdb-Partitionkey"] = pkHeader
		}

		return m
	}

	count := func(pkHeader string) float64 {
		rec = doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mh/docs", queryHdr(pkHeader),
			[]byte(`{"query":"SELECT VALUE COUNT(1) FROM c"}`))
		require.Equal(t, http.StatusOK, rec.Code)

		return decodeBody(t, rec)["Documents"].([]any)[0].(float64)
	}

	assert.InDelta(t, 4, count(""), 0)
	assert.InDelta(t, 4, count("[]"), 0)
	assert.InDelta(t, 3, count(`["t1"]`), 0, "prefix scoping")
	assert.InDelta(t, 2, count(`["t1","u1"]`), 0)
	assert.InDelta(t, 1, count(`["t1","u1","s1"]`), 0)
	assert.InDelta(t, 0, count(`["nope"]`), 0)

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mh/docs/1",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["t1","u1","s1"]`},
		[]byte(`{"id":"1","tenant":"t1","user":"u1","session":"OTHER"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "body key must agree with the request key")
}

func TestHierarchicalPartitionKeys_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "two paths multihash",
			body: `{"id":"a","partitionKey":{"paths":["/a","/b"],"kind":"MultiHash"}}`,
			want: http.StatusCreated,
		},
		{
			name: "two paths hash kind",
			body: `{"id":"a","partitionKey":{"paths":["/a","/b"],"kind":"Hash"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "four paths",
			body: `{"id":"a","partitionKey":{"paths":["/a","/b","/c","/d"],"kind":"MultiHash"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "duplicate paths",
			body: `{"id":"a","partitionKey":{"paths":["/a","/a"],"kind":"MultiHash"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "unprefixed path",
			body: `{"id":"a","partitionKey":{"paths":["/a","b"],"kind":"MultiHash"}}`,
			want: http.StatusBadRequest,
		},
		{name: "no paths", body: `{"id":"a","partitionKey":{"paths":[]}}`, want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDatabase(t, h)

			rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, []byte(tt.body))
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestQuery_PartitionKeyHeaderScopesSinglePathContainers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)
	createTestContainer(t, h)

	for _, d := range []string{`{"id":"1","pk":"a"}`, `{"id":"2","pk":"a"}`, `{"id":"3","pk":"b"}`} {
		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, []byte(d))
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	hdr := map[string]string{
		"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json",
		"X-Ms-Documentdb-Partitionkey": `["a"]`,
	}

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", hdr, []byte(`{"query":"SELECT * FROM c"}`))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.InDelta(t, 2, decodeBody(t, rec)["_count"], 0)
}

func TestTableAPI_SurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newTestHandler(t)
	createTestDatabase(t, src)
	createTestContainer(t, src)

	rec := doRequest(t, src, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, []byte(`{"id":"1","pk":"a"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, src, http.MethodPost, "/Tables", nil, []byte(`{"TableName":"widgets"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, src, http.MethodPost, "/widgets", nil, []byte(`{"PartitionKey":"p","RowKey":"r","Count":42}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	snap := src.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newTestHandler(t)
	require.NoError(t, dst.Restore(t.Context(), snap))

	rec = doRequest(t, dst, http.MethodGet, "/widgets(PartitionKey='p',RowKey='r')", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.InDelta(t, 42, decodeBody(t, rec)["Count"], 0)

	rec = doRequest(t, dst, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/1",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`}, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestTableAPI_RestoreWithoutTableStateClearsTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/Tables", nil, []byte(`{"TableName":"widgets"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	require.NoError(t, h.Restore(t.Context(), []byte(`{"databases":{},"version":1}`)))

	rec = doRequest(t, h, http.MethodGet, "/Tables", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, decodeBody(t, rec)["value"])
}

func TestContainerSnapshot_KeepsTTLPartitionPathsAndIndexing(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("d")
	require.NoError(t, err)
	_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{
		ID: "c", PartitionKeyPaths: []string{"/a", "/b"}, DefaultTTL: new(30),
		IndexingPolicy: []byte(`{"indexingMode":"none"}`),
	})
	require.NoError(t, err)

	restored := cosmosdb.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), b.Snapshot(t.Context())))

	info, err := restored.GetContainer("d", "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, info.PartitionKeyPaths)
	require.NotNil(t, info.DefaultTTL)
	assert.Equal(t, 30, *info.DefaultTTL)
	assert.JSONEq(t, `{"indexingMode":"none"}`, string(info.IndexingPolicy))
}

func TestTLS_ServesHTTPSWithSelfSignedCert(t *testing.T) {
	t.Parallel()

	h := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	h.Port = 0
	h.TLS = true

	require.NoError(t, h.StartWorker(t.Context()))

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(h.TLSCertPEM()))

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}
	t.Cleanup(client.CloseIdleConnections)

	url := fmt.Sprintf("https://localhost:%d/", h.Port)

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
		if err != nil {
			return false
		}

		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond)

	plain := fmt.Sprintf("http://127.0.0.1:%d/", h.Port)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, plain, http.NoBody)
	require.NoError(t, err)

	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		assert.NotEqual(t, http.StatusOK, resp.StatusCode, "plain HTTP must not be served on the TLS port")
	}
}

func TestTLS_AccountEndpointAdvertisesHTTPS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		tls  bool
	}{
		{name: "plain", want: "http://localhost:8081/"},
		{name: "tls", tls: true, want: "https://localhost:8081/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.TLS = tt.tls

			req := newReq(t, http.MethodGet, "/", "")
			req.Host = "localhost:8081"

			rec := serve(t, h, req)
			require.Equal(t, http.StatusOK, rec.Code)

			locations := decodeBody(t, rec)["writableLocations"].([]any)
			assert.Equal(t, tt.want, locations[0].(map[string]any)["databaseAccountEndpoint"])
		})
	}
}

func TestQueryContainers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	for _, id := range []string{"alpha", "beta"} {
		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil,
			fmt.Appendf(nil, `{"id":%q,"partitionKey":{"paths":["/pk"]}}`, id))
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	hdr := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/dbs/mydb/colls",
		hdr,
		[]byte(
			`{"query":"SELECT VALUE c.id FROM c WHERE STARTSWITH(c.id, @p)","parameters":[{"name":"@p","value":"be"}]}`,
		),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []any{"beta"}, decodeBody(t, rec)["DocumentCollections"])

	rec = doRequest(t, h, http.MethodPost, "/dbs/nope/colls", hdr, []byte(`{"query":"SELECT * FROM c"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/dbs", hdr, []byte(`{"query":"SELEC nope"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestExtractOperation_NewRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, path, header, want string
	}{
		{"query databases", http.MethodPost, "/dbs", "true", "QueryDatabases"},
		{"create database", http.MethodPost, "/dbs", "", "CreateDatabase"},
		{"query containers", http.MethodPost, "/dbs/d/colls", "true", "QueryContainers"},
		{"replace container", http.MethodPut, "/dbs/d/colls/c", "", "ReplaceContainer"},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := newReq(t, tt.method, tt.path, "")
			if tt.header != "" {
				req.Header.Set("X-Ms-Documentdb-Isquery", tt.header)
			}

			assert.Equal(t, tt.want, h.ExtractOperation(newEchoContextForRequest(t, req)))
		})
	}
}
