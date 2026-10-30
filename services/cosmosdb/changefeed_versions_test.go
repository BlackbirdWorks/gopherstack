package cosmosdb_test

import (
	"fmt"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const (
	avadFormat = "2021-09-15"
)

func avadHeaders(extra map[string]string) map[string]string {
	h := map[string]string{"A-Im": "Incremental feed", "X-Ms-Cosmos-Changefeed-Wire-Format-Version": avadFormat}
	maps.Copy(h, extra)

	return h
}

type versionEntry struct {
	op       string
	id       string
	hasPrev  bool
	curEmpty bool
	ttl      bool
}

func versionEntries(t *testing.T, r rawResponse) []versionEntry {
	t.Helper()

	docs, _ := r.object(t)["Documents"].([]any)
	out := make([]versionEntry, 0, len(docs))

	for _, d := range docs {
		m, _ := d.(map[string]any)
		meta, _ := m["metadata"].(map[string]any)
		cur, _ := m["current"].(map[string]any)
		_, hasPrev := m["previous"]
		ttl, _ := meta["timeToLiveExpired"].(bool)
		op, _ := meta["operationType"].(string)
		id, _ := meta["id"].(string)

		out = append(out, versionEntry{op: op, id: id, hasPrev: hasPrev, curEmpty: len(cur) == 0, ttl: ttl})
	}

	return out
}

func TestRaw_AllVersionsAndDeletes(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	pk := map[string]string{pkHeader: `["a"]`}

	start := s.do(http.MethodGet, "/dbs/d/colls/c/docs", avadHeaders(map[string]string{"If-None-Match": "*"}), "")
	require.Equal(t, http.StatusNotModified, start.status)

	cont := start.header.Get("ETag")

	require.Equal(
		t,
		http.StatusCreated,
		s.do(http.MethodPost, "/dbs/d/colls/c/docs", pk, `{"id":"x","pk":"a","v":1}`).status,
	)
	require.Equal(
		t,
		http.StatusOK,
		s.do(http.MethodPut, "/dbs/d/colls/c/docs/x", pk, `{"id":"x","pk":"a","v":2}`).status,
	)
	require.Equal(t, http.StatusNoContent, s.do(http.MethodDelete, "/dbs/d/colls/c/docs/x", pk, "").status)

	feed := s.do(http.MethodGet, "/dbs/d/colls/c/docs", avadHeaders(map[string]string{"If-None-Match": cont}), "")
	require.Equal(t, http.StatusOK, feed.status)

	assert.Equal(t, []versionEntry{
		{op: "create", id: "x"},
		{op: "replace", id: "x"},
		{op: "delete", id: "x", curEmpty: true},
	}, versionEntries(t, feed))

	first, _ := feed.object(t)["Documents"].([]any)
	meta, _ := first[0].(map[string]any)["metadata"].(map[string]any)
	assert.Equal(t, map[string]any{"pk": "a"}, meta["partitionKey"])
	assert.EqualValues(t, 1, meta["lsn"])

	second, _ := first[1].(map[string]any)["metadata"].(map[string]any)
	assert.EqualValues(t, 1, second["previousImageLSN"])

	caught := s.do(http.MethodGet, "/dbs/d/colls/c/docs", avadHeaders(map[string]string{
		"If-None-Match": feed.header.Get("ETag"),
	}), "")
	assert.Equal(t, http.StatusNotModified, caught.status)

	// The latest-version feed has no part of this: the document is gone.
	latest := s.do(http.MethodGet, "/dbs/d/colls/c/docs", map[string]string{"A-Im": "Incremental feed"}, "")
	assert.Equal(t, http.StatusNotModified, latest.status)
}

func TestRaw_AllVersionsStartRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		wantStatus int
	}{
		{name: "beginning_rejected", headers: avadHeaders(nil), wantStatus: http.StatusBadRequest},
		{
			name:       "continuation_zero",
			headers:    avadHeaders(map[string]string{"If-None-Match": `"0"`}),
			wantStatus: http.StatusOK,
		},
		{
			name: "start_time_rejected",
			headers: avadHeaders(map[string]string{
				"If-Modified-Since": time.Now().Add(-time.Hour).UTC().Format(time.RFC1123),
			}),
			wantStatus: http.StatusBadRequest,
		},
		{name: "bad_range", headers: avadHeaders(map[string]string{
			"If-None-Match": `"0"`, "X-Ms-Documentdb-Partitionkeyrangeid": "9",
		}), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated, s.create(`{"id":"a","pk":"a"}`).status)

			resp := s.do(http.MethodGet, "/dbs/d/colls/c/docs", tt.headers, "")
			assert.Equal(t, tt.wantStatus, resp.status)
		})
	}
}

func TestRaw_AllVersionsTTLExpiry(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	clk := &fakeClock{}
	cosmosdb.SetNowFunc(s.backend, clk.now)

	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"t","partitionKey":{"paths":["/pk"],"kind":"Hash"},"defaultTtl":5}`).status)

	pk := map[string]string{pkHeader: `["a"]`}
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/t/docs", pk, `{"id":"e","pk":"a"}`).status)

	clk.advance(10 * time.Second)
	require.Equal(t, 1, s.backend.SweepExpired())

	feed := s.do(http.MethodGet, "/dbs/d/colls/t/docs", avadHeaders(map[string]string{"If-None-Match": `"0"`}), "")
	require.Equal(t, http.StatusOK, feed.status)
	assert.Equal(t, []versionEntry{
		{op: "create", id: "e"},
		{op: "delete", id: "e", curEmpty: true, ttl: true},
	}, versionEntries(t, feed))
}

func TestRaw_AllVersionsRecreateOverExpired(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	clk := &fakeClock{}
	cosmosdb.SetNowFunc(s.backend, clk.now)

	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"t","partitionKey":{"paths":["/pk"],"kind":"Hash"},"defaultTtl":5}`).status)

	pk := map[string]string{pkHeader: `["a"]`}
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/t/docs", pk, `{"id":"e","pk":"a"}`).status)

	clk.advance(10 * time.Second)
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/t/docs", pk, `{"id":"e","pk":"a"}`).status)

	feed := s.do(http.MethodGet, "/dbs/d/colls/t/docs", avadHeaders(map[string]string{"If-None-Match": `"0"`}), "")
	assert.Equal(t, []versionEntry{
		{op: "create", id: "e"},
		{op: "delete", id: "e", curEmpty: true, ttl: true},
		{op: "create", id: "e"},
	}, versionEntries(t, feed))
}

func TestRaw_ChangeFeedPartitionRanges(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls",
		map[string]string{"X-Ms-Offer-Throughput": "25000"},
		`{"id":"m","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`).status)

	ranges := s.do(http.MethodGet, "/dbs/d/colls/m/pkranges", nil, "")
	list, _ := ranges.object(t)["PartitionKeyRanges"].([]any)
	require.Len(t, list, 3)

	const docs = 30

	for i := range docs {
		key := fmt.Sprintf("k%d", i)
		require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/m/docs",
			map[string]string{pkHeader: fmt.Sprintf(`[%q]`, key)}, fmt.Sprintf(`{"id":"i%d","pk":%q}`, i, key)).status)
	}

	for _, mode := range []struct {
		extra map[string]string
		ids   func(*testing.T, rawResponse) []string
		name  string
	}{
		{name: "latest", extra: nil, ids: feedIDs},
		{name: "all_versions", extra: avadHeaders(map[string]string{"If-None-Match": `"0"`}), ids: versionIDs},
	} {
		seen := map[string]int{}

		for _, r := range list {
			id, _ := r.(map[string]any)["id"].(string)
			headers := map[string]string{"A-Im": "Incremental feed", "X-Ms-Documentdb-Partitionkeyrangeid": id}

			maps.Copy(headers, mode.extra)

			resp := s.do(http.MethodGet, "/dbs/d/colls/m/docs", headers, "")
			if resp.status == http.StatusOK {
				for _, got := range mode.ids(t, resp) {
					seen[got]++
				}
			}
		}

		assert.Len(t, seen, docs, mode.name)

		for id, n := range seen {
			assert.Equal(t, 1, n, "%s: %s appears in exactly one range", mode.name, id)
		}
	}
}

func versionIDs(t *testing.T, r rawResponse) []string {
	t.Helper()

	entries := versionEntries(t, r)
	out := make([]string, 0, len(entries))

	for _, e := range entries {
		out = append(out, e.id)
	}

	return out
}

func TestChangeLog_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("d")
	require.NoError(t, err)
	_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
	require.NoError(t, err)

	_, err = b.CreateDocument("d", "c", decodeJSONBody(t, `{"id":"x","pk":"a","n":12345678901234567890}`), false)
	require.NoError(t, err)
	require.NoError(t, b.DeleteDocument("d", "c", `"a"`, "x", ""))

	snap := b.Snapshot(t.Context())
	assert.Contains(t, string(snap), `"ChangeLog"`)
	assert.Contains(t, string(snap), `12345678901234567890`)

	restored := cosmosdb.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))
	assert.JSONEq(t, string(snap), string(restored.Snapshot(t.Context())))
}
