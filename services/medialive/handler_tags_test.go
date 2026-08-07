package medialive_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestTags_LegacyStoreClearedOnDelete lives in whitebox_test.go: it needs
// direct access to the unexported legacy per-ARN tags map.
