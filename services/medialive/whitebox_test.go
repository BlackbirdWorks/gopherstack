package medialive

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tagStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("test.tagStoreCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}

func doWhiteboxRequest(t *testing.T, h *Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func decodeWhiteboxBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	return resp
}

// TestTags_LegacyStoreClearedOnDelete locks in a leak fix: resource
// families not covered by taggableResourceTags' fast path -- whose real
// Describe/List shapes have no inline Tags field, e.g. SdiSource,
// Reservation -- store their tags in the legacy per-ARN b.tags map. Before
// this fix, deleting one of these resources never removed its entry from
// b.tags, so every create+tag+delete cycle left a permanent ghost row for
// the lifetime of the backend. Exercised here via SdiSource. (Cluster
// previously exercised this test, but gopherstack-2mwl moved Cluster into
// taggableResourceTags' fast path since real DescribeClusterResult echoes
// Tags inline -- its tags now live on the resource struct, not b.tags.)
func TestTags_LegacyStoreClearedOnDelete(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(backend)

	baseline := tagStoreCount(backend)

	// CreateSdiSource itself now seeds b.tags[arn] (even with an empty map
	// when no tags are supplied), so the b.tags entry appears at creation,
	// not first-tagging.
	rec := doWhiteboxRequest(t, h, http.MethodPost, "/prod/sdiSources", map[string]any{"name": "tag-leak-sdisource"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := decodeWhiteboxBody(t, rec.Body.Bytes())
	sdiSource := created["sdiSource"].(map[string]any)
	sdiSourceID := sdiSource["id"].(string)
	sdiSourceARN := sdiSource["arn"].(string)
	assert.Equal(t, baseline+1, tagStoreCount(backend), "creating the sdiSource seeds one b.tags entry")

	rec = doWhiteboxRequest(t, h, http.MethodPost, "/prod/tags/"+sdiSourceARN, map[string]any{
		"tags": map[string]any{"env": "leak-test"},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, baseline+1, tagStoreCount(backend), "tagging an already-known ARN does not add a second entry")

	rec = doWhiteboxRequest(t, h, http.MethodDelete, "/prod/sdiSources/"+sdiSourceID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, baseline, tagStoreCount(backend),
		"deleting the sdiSource must remove its b.tags entry, not leave a ghost row")
}
