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
// families not covered by taggableResourceTags' fast path (Cluster,
// Node, SignalMap, CloudWatchAlarmTemplate(Group),
// EventBridgeRuleTemplate(Group), Reservation, Network, SdiSource,
// ChannelPlacementGroup) store their tags in the legacy per-ARN b.tags map.
// Before this fix, deleting one of these resources never removed its entry
// from b.tags, so every create+tag+delete cycle left a permanent ghost row
// for the lifetime of the backend. Exercised here via Cluster.
func TestTags_LegacyStoreClearedOnDelete(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(backend)

	baseline := tagStoreCount(backend)

	rec := doWhiteboxRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{"name": "tag-leak-cluster"})
	require.Equal(t, http.StatusCreated, rec.Code)
	cluster := decodeWhiteboxBody(t, rec.Body.Bytes())
	clusterID := cluster["id"].(string)
	clusterARN := cluster["arn"].(string)

	rec = doWhiteboxRequest(t, h, http.MethodPost, "/prod/tags/"+clusterARN, map[string]any{
		"tags": map[string]any{"env": "leak-test"},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, baseline+1, tagStoreCount(backend), "tagging the cluster adds one b.tags entry")

	rec = doWhiteboxRequest(t, h, http.MethodDelete, "/prod/clusters/"+clusterID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, baseline, tagStoreCount(backend),
		"deleting the cluster must remove its b.tags entry, not leave a ghost row")
}
