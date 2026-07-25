package medialive_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func TestTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Get ARN
	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	resourceARN := descResp["arn"].(string)

	// CreateTags
	rec = doRequest(t, h, http.MethodPost, "/prod/tags/"+resourceARN, map[string]any{
		"tags": map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/prod/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// DeleteTags
	req := httptest.NewRequest(http.MethodDelete, "/prod/tags/"+resourceARN+"?tagKeys=env", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, "/prod/tags/"+resourceARN, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}

// TestTags_StaySyncedWithResource guards against the pre-fix bug where the
// generic CreateTags/DeleteTags/ListTagsForResource endpoints wrote to a
// b.tags[ARN] map that was completely disconnected from the per-resource
// Tags field Describe/Create echo inline: tags set at CreateChannel never
// showed up in ListTagsForResource, and tags set via CreateTags never showed
// up in DescribeChannel.
func TestTags_StaySyncedWithResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Tags supplied at creation must be visible through the generic
	// tagging endpoint, not just echoed back on the create response.
	rec := doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
		"name":         "tag-sync-channel",
		"channelClass": "STANDARD",
		"tags":         map[string]any{"owner": "video-team"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	ch := created["channel"].(map[string]any)
	channelARN := ch["arn"].(string)

	rec = doRequest(t, h, http.MethodGet, "/prod/tags/"+channelARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["tags"].(map[string]any)
	assert.Equal(t, "video-team", tags["owner"])

	// Tags added via CreateTags must be echoed back by DescribeChannel, not
	// just visible through ListTagsForResource.
	rec = doRequest(t, h, http.MethodPost, "/prod/tags/"+channelARN, map[string]any{
		"tags": map[string]any{"cost-center": "1234"},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)

	channelID := ch["id"].(string)
	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	descTags := descResp["tags"].(map[string]any)
	assert.Equal(t, "video-team", descTags["owner"])
	assert.Equal(t, "1234", descTags["cost-center"])
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

	h := newTestHandler(t)
	backend := h.Backend.(*medialive.InMemoryBackend)

	baseline := medialive.TagStoreCount(backend)

	rec := doRequest(t, h, http.MethodPost, "/prod/clusters", map[string]any{"name": "tag-leak-cluster"})
	require.Equal(t, http.StatusCreated, rec.Code)
	cluster := decodeBody(t, rec.Body.Bytes())
	clusterID := cluster["id"].(string)
	clusterARN := cluster["arn"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/tags/"+clusterARN, map[string]any{
		"tags": map[string]any{"env": "leak-test"},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, baseline+1, medialive.TagStoreCount(backend), "tagging the cluster adds one b.tags entry")

	rec = doRequest(t, h, http.MethodDelete, "/prod/clusters/"+clusterID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, baseline, medialive.TagStoreCount(backend),
		"deleting the cluster must remove its b.tags entry, not leave a ghost row")
}
