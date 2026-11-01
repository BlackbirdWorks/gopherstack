package cosmosdb_test

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const ruPK = `["a"]`

func chargeOf(t *testing.T, rec *httptest.ResponseRecorder) float64 {
	t.Helper()

	v, err := strconv.ParseFloat(rec.Header().Get("X-Ms-Request-Charge"), 64)
	require.NoError(t, err)

	return v
}

func ruHandler(t *testing.T, indexing string) *cosmosdb.Handler {
	t.Helper()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	body := `{"id":"mycoll","partitionKey":{"paths":["/pk"],"kind":"Hash"}`
	if indexing != "" {
		body += `,"indexingPolicy":` + indexing
	}

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil, []byte(body+`}`))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	return h
}

// putSized stores a document whose point-read response is exactly size bytes.
func putSized(t *testing.T, h *cosmosdb.Handler, id string, size int) {
	t.Helper()

	probe := fmt.Sprintf(`{"id":%q,"pk":"a","pad":""}`, id)
	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, []byte(probe)).Code)

	empty := doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/"+id, map[string]string{
		"X-Ms-Documentdb-Partitionkey": ruPK,
	}, nil)
	require.Equal(t, http.StatusOK, empty.Code)

	pad := strings.Repeat("x", size-empty.Body.Len())
	body := fmt.Sprintf(`{"id":%q,"pk":"a","pad":%q}`, id, pad)

	rec := doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll/docs/"+id, map[string]string{
		"X-Ms-Documentdb-Partitionkey": ruPK,
	}, []byte(body))
	require.Equal(t, http.StatusOK, rec.Code)
}

func readDoc(t *testing.T, h *cosmosdb.Handler, id string, extra map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	hdr := map[string]string{"X-Ms-Documentdb-Partitionkey": ruPK}
	maps.Copy(hdr, extra)

	return doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll/docs/"+id, hdr, nil)
}

func TestRU_PointReadMatchesTheDocumentedExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want float64
	}{
		{name: "1 KB is 1 RU", size: 1024, want: 1},
		{name: "100 KB is 10 RU", size: 100 * 1024, want: 10},
		{name: "small items cost the 1 KB minimum", size: 300, want: 1},
		{name: "50 KB sits between", size: 50 * 1024, want: 1 + 9.0*49/99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ruHandler(t, "")
			putSized(t, h, "d", tt.size)

			assert.InDelta(t, tt.want, chargeOf(t, readDoc(t, h, "d", nil)), 0.02)
		})
	}
}

func TestRU_StrongAndBoundedStalenessReadsCostTwice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		level    string
		override string
		want     float64
	}{
		{name: "session", level: cosmosdb.ConsistencySession, want: 1},
		{name: "eventual", level: cosmosdb.ConsistencyEventual, want: 1},
		{name: "strong", level: cosmosdb.ConsistencyStrong, want: 2},
		{name: "bounded staleness", level: cosmosdb.ConsistencyBoundedStaleness, want: 2},
		{
			name:     "strong weakened to session per request",
			level:    cosmosdb.ConsistencyStrong,
			override: "Session",
			want:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ruHandler(t, "")
			putSized(t, h, "d", 1024)

			b, ok := h.Backend.(*cosmosdb.InMemoryBackend)
			require.True(t, ok)
			b.ConfigureAccountRegions(cosmosdb.AccountRegions{
				Consistency: &cosmosdb.ConsistencyPolicy{Level: tt.level},
				Regions:     []cosmosdb.AccountRegion{{Name: "East US"}},
			})

			extra := map[string]string{}
			if tt.override != "" {
				extra["X-Ms-Consistency-Level"] = tt.override
			}

			assert.InDelta(t, tt.want, chargeOf(t, readDoc(t, h, "d", extra)), 0.02)
		})
	}
}

func TestRU_WriteCostFollowsSizeAndIndexing(t *testing.T) {
	t.Parallel()

	const none = `{"indexingMode":"none"}`

	const onlyName = `{"indexingMode":"consistent","includedPaths":[{"path":"/name/?"}],"excludedPaths":[{"path":"/*"}]}`

	doc := func(id string) []byte {
		return fmt.Appendf(nil, `{"id":%q,"pk":"a","name":"n","a":1,"b":2,"c":{"d":3}}`, id)
	}

	create := func(h *cosmosdb.Handler, id string) float64 {
		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, doc(id))
		require.Equal(t, http.StatusCreated, rec.Code)

		return chargeOf(t, rec)
	}

	noIndex := create(ruHandler(t, none), "x")
	partial := create(ruHandler(t, onlyName), "x")
	full := create(ruHandler(t, ""), "x")

	assert.InDelta(t, 5.5, noIndex, 0.01, "an item that is not indexed costs the documented 5.5 RU")
	assert.Greater(t, partial, noIndex, "an indexed property costs more")
	assert.Greater(t, full, partial, "indexing more properties costs more")
	assert.InDelta(t, 5.5+0.3*6, full, 0.01, "id, pk, name, a, b and c/d are indexed by default")

	h := ruHandler(t, none)
	create(h, "y")

	replace := doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll/docs/y",
		map[string]string{"X-Ms-Documentdb-Partitionkey": ruPK}, doc("y"))
	require.Equal(t, http.StatusOK, replace.Code)
	assert.InDelta(t, 2*5.5, chargeOf(t, replace), 0.01, "a replace costs twice an insert")

	big := ruHandler(t, none)
	putBig := func() float64 {
		body := fmt.Sprintf(`{"id":"b","pk":"a","pad":%q}`, strings.Repeat("x", 100*1024))
		rec := doRequest(t, big, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", nil, []byte(body))
		require.Equal(t, http.StatusCreated, rec.Code)

		return chargeOf(t, rec)
	}()

	assert.InDelta(t, 5.5*10, putBig, 1, "writes grow with item size like reads (1 -> 10 RU at 100 KB)")
}

func TestRU_QueryChargeFollowsRetrievedDocuments(t *testing.T) {
	t.Parallel()

	query := func(h *cosmosdb.Handler, sql string) float64 {
		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", map[string]string{
			"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json",
		}, []byte(`{"query":"`+sql+`"}`))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		return chargeOf(t, rec)
	}

	t.Run("one retrieved 10 KB document is the documented 3.19 RU", func(t *testing.T) {
		t.Parallel()

		h := ruHandler(t, "")
		b, _ := h.Backend.(*cosmosdb.InMemoryBackend)
		_, err := b.CreateDocument("mydb", "mycoll", map[string]any{
			"id": "big", "pk": "a", "pad": strings.Repeat("x", 9963-len(`{"id":"big","pk":"a","pad":""}`)),
		}, false)
		require.NoError(t, err)

		assert.InDelta(t, 3.19, query(h, "SELECT * FROM c"), 0.05)
	})

	t.Run("a thousand 1 KB items cost about a thousand RU", func(t *testing.T) {
		t.Parallel()

		h := ruHandler(t, "")
		b, _ := h.Backend.(*cosmosdb.InMemoryBackend)

		for i := range 1000 {
			id := fmt.Sprintf("i%04d", i)
			pad := strings.Repeat("x", 1024-len(fmt.Sprintf(`{"id":%q,"pk":"a","pad":""}`, id)))
			_, err := b.CreateDocument("mydb", "mycoll", map[string]any{"id": id, "pk": "a", "pad": pad}, false)
			require.NoError(t, err)
		}

		assert.InDelta(t, 1000, query(h, "SELECT * FROM c"), 5)
	})

	t.Run("an indexed filter loads only what it keeps", func(t *testing.T) {
		t.Parallel()

		h := ruHandler(t, "")
		b, _ := h.Backend.(*cosmosdb.InMemoryBackend)

		for i := range 20 {
			_, err := b.CreateDocument(
				"mydb",
				"mycoll",
				map[string]any{"id": fmt.Sprintf("i%d", i), "pk": "a", "n": i},
				false,
			)
			require.NoError(t, err)
		}

		all := query(h, "SELECT * FROM c")
		one := query(h, "SELECT * FROM c WHERE c.n = 3")
		assert.Less(t, one, all)
		assert.InDelta(t, 1.4+1, one, 0.01)
	})

	t.Run("a filter on an excluded path loads every document", func(t *testing.T) {
		t.Parallel()

		h := ruHandler(
			t,
			`{"indexingMode":"consistent","includedPaths":[{"path":"/n/?"}],"excludedPaths":[{"path":"/*"}]}`,
		)
		b, _ := h.Backend.(*cosmosdb.InMemoryBackend)

		for i := range 20 {
			_, err := b.CreateDocument(
				"mydb",
				"mycoll",
				map[string]any{"id": fmt.Sprintf("i%d", i), "pk": "a", "n": i, "m": i},
				false,
			)
			require.NoError(t, err)
		}

		rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls/mycoll/docs", map[string]string{
			"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json",
			"X-Ms-Documentdb-Query-Enable-Scan": "true",
		}, []byte(`{"query":"SELECT * FROM c WHERE c.m = 3"}`))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.InDelta(t, 1.4+20, chargeOf(t, rec), 0.05)
	})
}

func TestRU_CrossPartitionQueriesPayPerPartition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		scope string
		want  float64
	}{
		{name: "cross partition", want: 1.4 + 1 + 2.5*3},
		{name: "scoped by the partition key", scope: ruPK, want: 1.4 + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, false, false)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", manualHeader(30000))
			require.Equal(t, http.StatusCreated, s.putPK("c", "k", 0).status)

			hdr := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}
			if tt.scope != "" {
				hdr["X-Ms-Documentdb-Partitionkey"] = `["k"]`
			}

			resp := s.do(http.MethodPost, "/dbs/d/colls/c/docs", hdr, `{"query":"SELECT * FROM c"}`)
			require.Equal(t, http.StatusOK, resp.status)

			got, err := strconv.ParseFloat(resp.header.Get("X-Ms-Request-Charge"), 64)
			require.NoError(t, err)
			assert.InDelta(t, tt.want, got, 0.1)
		})
	}
}
