package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

func newAsyncIndexerStack(t *testing.T) (*stack, chan func()) {
	t.Helper()

	jobs := make(chan func(), 8)
	s := newStackExec(t, "standard", func(f func()) { jobs <- f })
	blobs := newFakeBlobs()
	blobs.put("a.txt", "hello world")
	blobs.put("b.txt", "goodbye moon")
	s.b.SetBlobSource(blobs)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource(), http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)

	return s, jobs
}

func TestIndexerRunsAsynchronously(t *testing.T) {
	t.Parallel()

	s, jobs := newAsyncIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	st := s.status(t, "ix")
	last, _ := st["lastResult"].(map[string]any)
	assert.Equal(t, "inProgress", last["status"])
	assert.Nil(t, last["endTime"], "a run in progress has no end time")

	hist, _ := st["executionHistory"].([]any)
	require.Len(t, hist, 1)
	assert.Equal(t, "inProgress", hist[0].(map[string]any)["status"])

	assert.Empty(t, s.searchIDs(t, "hello"), "nothing is indexed until the run executes")

	cases := []struct {
		method string
		path   string
		name   string
	}{
		{http.MethodPost, "/indexers('ix')/search.run?", "second run"},
		{http.MethodPost, "/indexers('ix')/search.reset?", "reset"},
	}

	for _, tc := range cases {
		r := s.admin2(t, tc.method, tc.path+apiVer, nil)
		assert.Equal(t, http.StatusConflict, r.status, tc.name+": "+string(r.raw))
	}

	(<-jobs)()

	done, _ := s.status(t, "ix")["lastResult"].(map[string]any)
	assert.Equal(t, "success", done["status"])
	assert.EqualValues(t, 2, done["itemsProcessed"])
	assert.NotNil(t, done["endTime"])
	assert.Equal(t, []string{"a.txt"}, s.searchIDs(t, "hello"))

	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)
	(<-jobs)()
}

func TestIndexerInProgressInterruptedByRestore(t *testing.T) {
	t.Parallel()

	s, _ := newAsyncIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	restored := azuresearch.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), s.b.Snapshot(t.Context())))

	t.Cleanup(restored.WaitRuns)

	again := &stack{b: restored, admin: s.admin, query: s.query, clock: s.clock, url: serve(t, restored)}
	last, _ := again.status(t, "ix")["lastResult"].(map[string]any)
	assert.Equal(t, "transientFailure", last["status"])

	again.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)
}
