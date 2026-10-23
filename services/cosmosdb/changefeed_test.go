package cosmosdb_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func feedIDs(t *testing.T, r rawResponse) []string {
	t.Helper()

	docs, _ := r.object(t)["Documents"].([]any)
	ids := make([]string, 0, len(docs))

	for _, d := range docs {
		m, _ := d.(map[string]any)
		id, _ := m["id"].(string)
		ids = append(ids, id)
	}

	return ids
}

func TestRaw_ChangeFeed(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	feed := func(extra map[string]string) rawResponse {
		h := map[string]string{"A-Im": "Incremental feed"}
		maps.Copy(h, extra)

		return s.do(http.MethodGet, "/dbs/d/colls/c/docs", h, "")
	}

	empty := feed(nil)
	assert.Equal(t, http.StatusNotModified, empty.status)
	assert.Equal(t, `"0"`, empty.header.Get("ETag"))

	for _, d := range []string{`{"id":"a1","pk":"a"}`, `{"id":"a2","pk":"a"}`} {
		require.Equal(t, http.StatusCreated, s.create(d).status)
	}

	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls/c/docs",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["b"]`}, `{"id":"b1","pk":"b"}`).status)

	first := feed(nil)
	require.Equal(t, http.StatusOK, first.status)
	assert.Equal(t, []string{"a1", "a2", "b1"}, feedIDs(t, first))
	assert.Equal(t, `"3"`, first.header.Get("ETag"))

	docs, _ := first.object(t)["Documents"].([]any)
	assert.EqualValues(t, 1, docs[0].(map[string]any)["_lsn"])

	caught := feed(map[string]string{"If-None-Match": first.header.Get("ETag")})
	assert.Equal(t, http.StatusNotModified, caught.status)
	assert.Equal(t, `"3"`, caught.header.Get("ETag"))

	// An update moves the document to the end of the feed; a delete is not part of the latest-version feed.
	require.Equal(t, http.StatusOK, s.do(http.MethodPut, "/dbs/d/colls/c/docs/a1",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`}, `{"id":"a1","pk":"a","v":2}`).status)
	require.Equal(t, http.StatusNoContent, s.do(http.MethodDelete, "/dbs/d/colls/c/docs/a2",
		map[string]string{"X-Ms-Documentdb-Partitionkey": `["a"]`}, "").status)

	next := feed(map[string]string{"If-None-Match": first.header.Get("ETag")})
	require.Equal(t, http.StatusOK, next.status)
	assert.Equal(t, []string{"a1"}, feedIDs(t, next))

	all := feed(nil)
	assert.Equal(t, []string{"b1", "a1"}, feedIDs(t, all))

	scoped := feed(map[string]string{"X-Ms-Documentdb-Partitionkey": `["b"]`})
	assert.Equal(t, []string{"b1"}, feedIDs(t, scoped))

	paged := feed(map[string]string{"X-Ms-Max-Item-Count": "1"})
	assert.Equal(t, []string{"b1"}, feedIDs(t, paged))

	rest := feed(map[string]string{"If-None-Match": paged.header.Get("ETag")})
	assert.Equal(t, []string{"a1"}, feedIDs(t, rest))

	now := feed(map[string]string{"If-None-Match": "*"})
	assert.Equal(t, http.StatusNotModified, now.status)

	require.Equal(t, http.StatusCreated, s.create(`{"id":"a3","pk":"a"}`).status)

	fresh := feed(map[string]string{"If-None-Match": now.header.Get("ETag")})
	assert.Equal(t, []string{"a3"}, feedIDs(t, fresh))
}

func TestRaw_ChangeFeedStartTimeAndErrors(t *testing.T) {
	t.Parallel()

	since := func(d time.Duration) map[string]string {
		return map[string]string{"If-Modified-Since": time.Now().Add(d).UTC().Format(time.RFC1123)}
	}
	rangeID := func(id string) map[string]string {
		return map[string]string{"X-Ms-Documentdb-Partitionkeyrangeid": id}
	}

	tests := []struct {
		headers    map[string]string
		name       string
		wantStatus int
		wantIDs    int
	}{
		{name: "start in the past", headers: since(-time.Hour), wantStatus: 200, wantIDs: 1},
		{name: "start in the future", headers: since(time.Hour), wantStatus: 304},
		{name: "bad start time", headers: map[string]string{"If-Modified-Since": "yesterday"}, wantStatus: 400},
		{name: "bad continuation", headers: map[string]string{"If-None-Match": `"abc"`}, wantStatus: 400},
		{name: "unknown range", headers: rangeID("7"), wantStatus: 400},
		{name: "known range", headers: rangeID("0"), wantStatus: 200, wantIDs: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated, s.create(`{"id":"a1","pk":"a"}`).status)

			h := map[string]string{"A-Im": "Incremental feed"}
			maps.Copy(h, tt.headers)

			resp := s.do(http.MethodGet, "/dbs/d/colls/c/docs", h, "")
			assert.Equal(t, tt.wantStatus, resp.status, string(resp.body))

			if tt.wantIDs > 0 {
				assert.Len(t, feedIDs(t, resp), tt.wantIDs)
			}
		})
	}
}

func TestRaw_PartitionKeyRanges(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)

	resp := s.do(http.MethodGet, "/dbs/d/colls/c/pkranges", nil, "")
	require.Equal(t, http.StatusOK, resp.status)

	ranges, _ := resp.object(t)["PartitionKeyRanges"].([]any)
	require.Len(t, ranges, 1)
	assert.Equal(t, "FF", ranges[0].(map[string]any)["maxExclusive"])

	cached := s.do(
		http.MethodGet,
		"/dbs/d/colls/c/pkranges",
		map[string]string{"If-None-Match": resp.header.Get("ETag")},
		"",
	)
	assert.Equal(t, http.StatusNotModified, cached.status)

	assert.Equal(t, http.StatusNotFound, s.do(http.MethodGet, "/dbs/d/colls/missing/pkranges", nil, "").status)
}

func TestSDK_ReadChangeFeed(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	c := s.container()
	ctx := t.Context()
	opts := func(token string) *azcosmos.ChangeFeedOptions {
		o := &azcosmos.ChangeFeedOptions{FeedRange: &azcosmos.FeedRange{MinInclusive: "", MaxExclusive: "FF"}}
		if token != "" {
			o.Continuation = &token
		}

		return o
	}

	for _, id := range []string{"x1", "x2"} {
		_, err := c.CreateItem(ctx, azcosmos.NewPartitionKeyString("a"), []byte(`{"id":"`+id+`","pk":"a"}`), nil)
		require.NoError(t, err)
	}

	first, err := c.ReadChangeFeed(ctx, opts(""))
	require.NoError(t, err)
	assert.Len(t, first.Items, 2)
	require.NotEmpty(t, first.ContinuationToken)

	caughtUp, err := c.ReadChangeFeed(ctx, opts(first.ContinuationToken))
	require.NoError(t, err)
	assert.Empty(t, caughtUp.Items)
	assert.Equal(t, http.StatusNotModified, caughtUp.RawResponse.StatusCode)

	_, err = c.UpsertItem(ctx, azcosmos.NewPartitionKeyString("a"), []byte(`{"id":"x1","pk":"a","v":1}`), nil)
	require.NoError(t, err)

	delta, err := c.ReadChangeFeed(ctx, opts(caughtUp.ContinuationToken))
	require.NoError(t, err)
	require.Len(t, delta.Items, 1)
	assert.JSONEq(t, `{"id":"x1","pk":"a","v":1}`, stripSystem(t, delta.Items[0]))

	scoped, err := c.ReadChangeFeed(ctx, &azcosmos.ChangeFeedOptions{
		FeedRange:    &azcosmos.FeedRange{MinInclusive: "", MaxExclusive: "FF"},
		PartitionKey: new(azcosmos.NewPartitionKeyString("nobody")),
	})
	require.NoError(t, err)
	assert.Empty(t, scoped.Items)

	fromNow, err := c.ReadChangeFeed(ctx, &azcosmos.ChangeFeedOptions{
		FeedRange: &azcosmos.FeedRange{MinInclusive: "", MaxExclusive: "FF"},
		StartFrom: new(time.Now().Add(time.Hour)),
	})
	require.NoError(t, err)
	assert.Empty(t, fromNow.Items)
}
