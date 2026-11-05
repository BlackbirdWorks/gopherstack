package azuresearch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

func TestSnapshotRestoreKeepsEverything(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)
	s.must(t, http.MethodPut, "/synonymmaps('m')?"+apiVer, map[string]any{"name": "m", "synonyms": "a, b"}, http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource("ds"), http.StatusCreated)

	snap := s.b.Snapshot(t.Context())

	restored := azuresearch.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	keys, err := restored.Keys(testService)
	require.NoError(t, err)
	assert.Equal(t, s.admin, keys.Primary, "keys survive a restore")

	again := &stack{b: restored, admin: s.admin, query: s.query, clock: s.clock}
	again.url = serve(t, restored)

	r := searchGet(t, again, url.Values{"search": {"motel"}, "$select": {"HotelId"}})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))
	assert.ElementsMatch(t, []string{"1", "2"}, idsOf(r), "analysis caches are rebuilt on restore")

	count := again.must(t, http.MethodGet, "/indexes('hotels')/docs/$count?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "4", string(count.raw))

	again.must(t, http.MethodGet, "/synonymmaps('m')?"+apiVer, nil, http.StatusOK)
	again.must(t, http.MethodGet, "/datasources('ds')?"+apiVer, nil, http.StatusOK)

	idx := searchGet(t, again, url.Values{"$filter": {"Rooms/any(r: r/BaseRate lt 100)"}, "$select": {"HotelId"}})
	assert.ElementsMatch(t, []string{"1", "3"}, idsOf(idx))
}

func TestRestoreRejectsCorruptSnapshots(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
	}{
		{"null service", `{"version":1,"services":{"a":null}}`},
		{"null index", `{"version":1,"services":{"a":{"indexes":{"i":null}}}}`},
		{"null document", `{"version":1,"services":{"a":{"indexes":{"i":{"def":{"name":"i","fields":[{"name":"id","type":"Edm.String","key":true}]},"docs":{"k":null}}}}}}`},
		{"invalid index definition", `{"version":1,"services":{"a":{"indexes":{"i":{"def":{"name":"i","fields":[]},"docs":{}}}}}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := azuresearch.NewInMemoryBackend()
			require.Error(t, b.Restore(t.Context(), []byte(tc.raw)))
		})
	}

	b := azuresearch.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":99,"services":{}}`)), "unknown versions start empty")
	assert.Empty(t, b.ServiceNames())
}
