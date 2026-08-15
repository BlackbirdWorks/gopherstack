package workspaces_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Tag limit enforcement
// ---------------------------------------------------------------------------

func TestCreateTags_ExceedsLimit_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	// First batch: 50 tags.
	tags50 := make([]map[string]any, 50)
	for i := range tags50 {
		tags50[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       tags50,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "50 tags must be accepted")

	// One more tag should push over the limit.
	rec = doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "overflow", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "51st tag must be rejected")
}

func TestCreateTags_Update_DoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	// Add 50 tags.
	tags50 := make([]map[string]any, 50)
	for i := range tags50 {
		tags50[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       tags50,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Updating an existing key should succeed (not counted as a new tag).
	rec = doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "key0", "Value": "updated"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "updating existing tag must succeed even at limit")
}

// ---------------------------------------------------------------------------
// Tag key validation
// ---------------------------------------------------------------------------

func TestCreateTags_EmptyKey_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)
	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "", "Value": "val"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "empty tag key must return 400")
}

func TestCreateTags_EmptyResourceId_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": "",
		"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "empty ResourceId must return 400")
}

// ---------------------------------------------------------------------------
// Tags via CreateTags visible in DescribeTags, absent from DescribeWorkspaces
// ---------------------------------------------------------------------------

// TestCreateTags_VisibleInDescribeTags verifies that tags added via
// CreateTags after workspace creation are readable back via DescribeTags --
// the real API's tag-read path (types.Workspace has no Tags member; a
// caller reads tags via a separate DescribeTags call).
func TestCreateTags_VisibleInDescribeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})

	rec := doTargetRequest(t, h, "DescribeTags", map[string]any{
		"ResourceId": wsID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TagList []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"TagList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags := make(map[string]string, len(resp.TagList))
	for _, tag := range resp.TagList {
		tags[tag.Key] = tag.Value
	}
	assert.Equal(t, "prod", tags["env"], "CreateTags changes must be visible in DescribeTags")
	assert.Equal(t, "platform", tags["team"])
}

// ---------------------------------------------------------------------------
// DeleteTags removes tags from DescribeTags
// ---------------------------------------------------------------------------

// TestDeleteTags_RemovedFromDescribeTags verifies that DeleteTags removes
// tags from the DescribeTags response.
func TestDeleteTags_RemovedFromDescribeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "keep", "Value": "yes"},
		},
	})

	doTargetRequest(t, h, "DeleteTags", map[string]any{
		"ResourceId": wsID,
		"TagKeys":    []string{"env"},
	})

	rec := doTargetRequest(t, h, "DescribeTags", map[string]any{
		"ResourceId": wsID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TagList []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"TagList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags := make(map[string]string, len(resp.TagList))
	for _, tag := range resp.TagList {
		tags[tag.Key] = tag.Value
	}
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "deleted tag must not appear in DescribeTags")
	assert.Equal(t, "yes", tags["keep"], "non-deleted tag must still appear")
}

// TestDescribeWorkspaces_NoTagsField is a raw-body assertion that a
// Workspace item never carries a Tags key: types.Workspace (aws-sdk-go-v2/
// service/workspaces@v1.73.1, types/types.go) has no such member -- tags are
// read back only via DescribeTags. An SDK client silently discards unknown
// response keys, so a client-typed test would pass even with the field
// still fabricated onto the wire.
func TestDescribeWorkspaces_NoTagsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wsID := createWorkspace(t, h)

	doTargetRequest(t, h, "CreateTags", map[string]any{
		"ResourceId": wsID,
		"Tags":       []map[string]any{{"Key": "env", "Value": "prod"}},
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaces", map[string]any{
		"WorkspaceIds": []string{wsID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Workspaces []map[string]json.RawMessage `json:"Workspaces"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Workspaces, 1)
	assert.NotContains(t, resp.Workspaces[0], "Tags")
}

// ---------------------------------------------------------------------------
// Tags from CreateWorkspaces visible in DescribeTags
// ---------------------------------------------------------------------------

// TestCreateWorkspaces_Tags_VisibleInDescribeTags verifies that tags provided
// at workspace creation time are retrievable via DescribeTags.
func TestCreateWorkspaces_Tags_VisibleInDescribeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-abc123"})

	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-abc123",
				"BundleId":    "wsb-bh8rsxt14",
				"Tags":        []map[string]any{{"Key": "project", "Value": "gamma"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	pending, _ := createResp["PendingRequests"].([]any)
	require.Len(t, pending, 1)
	wsID := pending[0].(map[string]any)["WorkspaceId"].(string)

	descRec := doTargetRequest(t, h, "DescribeTags", map[string]any{
		"ResourceId": wsID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &tagResp))
	tagList, _ := tagResp["TagList"].([]any)

	found := false
	for _, item := range tagList {
		tag := item.(map[string]any)
		if tag["Key"] == "project" && tag["Value"] == "gamma" {
			found = true
		}
	}

	assert.True(t, found, "tags from CreateWorkspaces must be visible via DescribeTags")
}
