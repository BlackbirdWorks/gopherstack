package cosmosdb_test

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func ttlEpoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

type fakeClock struct{ offset atomic.Int64 }

func (f *fakeClock) now() time.Time { return ttlEpoch().Add(time.Duration(f.offset.Load())) }

func (f *fakeClock) advance(d time.Duration) { f.offset.Add(int64(d)) }

func ttlBackend(t *testing.T, defaultTTL *int) (*cosmosdb.InMemoryBackend, *fakeClock) {
	t.Helper()

	b := cosmosdb.NewInMemoryBackend()
	clk := &fakeClock{}
	cosmosdb.SetNowFunc(b, clk.now)

	_, err := b.CreateDatabase("d")
	require.NoError(t, err)
	_, err = b.CreateContainer("d", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk", DefaultTTL: defaultTTL})
	require.NoError(t, err)

	return b, clk
}

func TestTTL_Expiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		defaultTTL *int
		body       string
		advance    time.Duration
		wantLive   bool
	}{
		{
			name:       "default ttl not yet elapsed",
			defaultTTL: new(10),
			body:       `{"id":"x","pk":"a"}`,
			advance:    9 * time.Second,
			wantLive:   true,
		},
		{name: "default ttl elapsed", defaultTTL: new(10), body: `{"id":"x","pk":"a"}`, advance: 10 * time.Second},
		{
			name:       "item ttl overrides shorter",
			defaultTTL: new(100),
			body:       `{"id":"x","pk":"a","ttl":3}`,
			advance:    4 * time.Second,
		},
		{
			name:       "item ttl overrides longer",
			defaultTTL: new(5),
			body:       `{"id":"x","pk":"a","ttl":50}`,
			advance:    20 * time.Second,
			wantLive:   true,
		},
		{
			name:       "item ttl minus one never expires",
			defaultTTL: new(1),
			body:       `{"id":"x","pk":"a","ttl":-1}`,
			advance:    time.Hour,
			wantLive:   true,
		},
		{
			name:       "container minus one with item ttl",
			defaultTTL: new(-1),
			body:       `{"id":"x","pk":"a","ttl":2}`,
			advance:    3 * time.Second,
		},
		{
			name:       "container minus one without item ttl",
			defaultTTL: new(-1),
			body:       `{"id":"x","pk":"a"}`,
			advance:    time.Hour,
			wantLive:   true,
		},
		{
			name:     "ttl disabled ignores item ttl",
			body:     `{"id":"x","pk":"a","ttl":1}`,
			advance:  time.Hour,
			wantLive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clk := ttlBackend(t, tt.defaultTTL)
			_, err := b.CreateDocument("d", "c", decodeJSONBody(t, tt.body), false)
			require.NoError(t, err)

			clk.advance(tt.advance)

			_, getErr := b.GetDocument("d", "c", `"a"`, "x")
			listed, listErr := b.ListDocuments("d", "c")
			require.NoError(t, listErr)

			queried, queryErr := b.QueryDocuments("d", "c", "SELECT VALUE c.id FROM c", nil)
			require.NoError(t, queryErr)

			if tt.wantLive {
				require.NoError(t, getErr)
				assert.Len(t, listed, 1)
				assert.Len(t, queried, 1)

				return
			}

			require.ErrorIs(t, getErr, cosmosdb.ErrDocumentNotFound)
			assert.Empty(t, listed)
			assert.Empty(t, queried)
		})
	}
}

func TestTTL_ExpiredReadsAsAbsentForWrites(t *testing.T) {
	t.Parallel()

	b, clk := ttlBackend(t, new(5))
	_, err := b.CreateDocument("d", "c", decodeJSONBody(t, `{"id":"x","pk":"a","v":1}`), false)
	require.NoError(t, err)

	clk.advance(6 * time.Second)

	require.ErrorIs(t, b.DeleteDocument("d", "c", `"a"`, "x", ""), cosmosdb.ErrDocumentNotFound)

	_, err = b.CreateDocument("d", "c", decodeJSONBody(t, `{"id":"x","pk":"a","v":2}`), false)
	require.NoError(t, err, "creating over an expired id must not conflict")

	info, err := b.GetDocument("d", "c", `"a"`, "x")
	require.NoError(t, err)
	assert.Equal(t, json.Number("2"), info.Body["v"])
}

func TestTTL_SweepRemovesExpired(t *testing.T) {
	t.Parallel()

	b, clk := ttlBackend(t, new(5))

	for _, body := range []string{`{"id":"a","pk":"p"}`, `{"id":"b","pk":"p","ttl":-1}`, `{"id":"c","pk":"p","ttl":60}`} {
		_, err := b.CreateDocument("d", "c", decodeJSONBody(t, body), false)
		require.NoError(t, err)
	}

	assert.Equal(t, 0, b.SweepExpired())

	clk.advance(6 * time.Second)

	assert.Equal(t, 3, cosmosdb.DocumentCount(b, "d", "c"))
	assert.Equal(t, 1, b.SweepExpired())
	assert.Equal(t, 2, cosmosdb.DocumentCount(b, "d", "c"))

	clk.advance(2 * time.Minute)

	assert.Equal(t, 1, b.SweepExpired())
	assert.Equal(t, 1, cosmosdb.DocumentCount(b, "d", "c"))
}

func TestTTL_InvalidValues(t *testing.T) {
	t.Parallel()

	b, _ := ttlBackend(t, new(10))

	invalid := []string{
		`{"id":"x","pk":"a","ttl":0}`, `{"id":"x","pk":"a","ttl":-5}`,
		`{"id":"x","pk":"a","ttl":"1"}`, `{"id":"x","pk":"a","ttl":1.5}`,
	}

	for _, body := range invalid {
		_, err := b.CreateDocument("d", "c", decodeJSONBody(t, body), false)
		require.ErrorIs(t, err, cosmosdb.ErrInvalidDocument, body)
	}

	for _, ttl := range []int{0, -2} {
		_, err := b.CreateContainer(
			"d",
			cosmosdb.ContainerSpec{ID: "bad", PartitionKeyPath: "/pk", DefaultTTL: &ttl},
		)
		require.ErrorIs(t, err, cosmosdb.ErrInvalidDefaultTTL)
	}
}

func TestTTL_ContainerHTTPLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestDatabase(t, h)

	rec := doRequest(t, h, http.MethodPost, "/dbs/mydb/colls", nil,
		[]byte(`{"id":"mycoll","partitionKey":{"paths":["/pk"]},"defaultTtl":60}`))
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.InDelta(t, 60, decodeBody(t, rec)["defaultTtl"], 0)

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll", nil,
		[]byte(`{"id":"mycoll","partitionKey":{"paths":["/pk"]},"defaultTtl":-1}`))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll", nil, nil)
	assert.InDelta(t, -1, decodeBody(t, rec)["defaultTtl"], 0)

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll", nil,
		[]byte(`{"id":"mycoll","partitionKey":{"paths":["/other"]}}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "partition key is immutable")

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll", nil,
		[]byte(`{"id":"mycoll","partitionKey":{"paths":["/pk"]}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/dbs/mydb/colls/mycoll", nil, nil)
	assert.NotContains(t, decodeBody(t, rec), "defaultTtl", "replace without defaultTtl disables TTL")

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/nope", nil,
		[]byte(`{"id":"nope","partitionKey":{"paths":["/pk"]}}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodPut, "/dbs/mydb/colls/mycoll", nil,
		[]byte(`{"id":"mycoll","partitionKey":{"paths":["/pk"]},"defaultTtl":0}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTTL_JanitorSweepsAndStops(t *testing.T) {
	t.Parallel()

	b, clk := ttlBackend(t, new(1))
	_, err := b.CreateDocument("d", "c", decodeJSONBody(t, `{"id":"x","pk":"a"}`), false)
	require.NoError(t, err)

	h := cosmosdb.NewHandler(b)
	h.Port = 0
	h.SweepInterval = 5 * time.Millisecond

	require.NoError(t, h.StartWorker(t.Context()))

	t.Cleanup(func() { h.Shutdown(t.Context()) })

	clk.advance(5 * time.Second)

	require.Eventually(t, func() bool { return cosmosdb.DocumentCount(b, "d", "c") == 0 },
		5*time.Second, 5*time.Millisecond, "janitor must physically remove the expired document")

	h.Shutdown(t.Context())
	h.Shutdown(t.Context())
}
