package directoryservice_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

func TestDirectoryService_Tags(t *testing.T) {
	t.Parallel()

	createDir := func(h *directoryservice.Handler) string {
		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var resp map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		return resp["DirectoryId"].(string)
	}

	t.Run("AddTagsToResource and ListTagsForResource round-trip", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)

		addRec := doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "test"}},
		})
		assert.Equal(t, http.StatusOK, addRec.Code)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		assert.Equal(t, http.StatusOK, listRec.Code)

		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags, ok := listResp["Tags"].([]any)
		require.True(t, ok)
		require.Len(t, tags, 1)
		tag := tags[0].(map[string]any)
		assert.Equal(t, "env", tag["Key"])
		assert.Equal(t, "test", tag["Value"])
	})

	t.Run("RemoveTagsFromResource removes specified keys", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "env", "Value": "test"},
				{"Key": "team", "Value": "ops"},
			},
		})

		removeRec := doRequest(t, h, "RemoveTagsFromResource", map[string]any{
			"ResourceId": dirID,
			"TagKeys":    []string{"env"},
		})
		assert.Equal(t, http.StatusOK, removeRec.Code)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags := listResp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "team", tags[0].(map[string]any)["Key"])
	})

	t.Run("AddTagsToResource unknown resource returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": "d-0000000000",
			"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("tags from CreateDirectory are preserved", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name":     "corp.example.com",
			"Password": "Admin1234!",
			"Size":     "Small",
			"Tags":     []map[string]any{{"Key": "created", "Value": "yes"}},
		})
		var createResp map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
		dirID := createResp["DirectoryId"].(string)

		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		var listResp map[string]any
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
		tags := listResp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "created", tags[0].(map[string]any)["Key"])
	})
}

func TestListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("pagination with limit returns correct page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		// Add 5 tags
		tags := make([]map[string]any, 5)
		for i := range 5 {
			tags[i] = map[string]any{
				"Key":   fmt.Sprintf("tag%02d", i),
				"Value": fmt.Sprintf("val%d", i),
			}
		}
		doRequest(t, h, "AddTagsToResource", map[string]any{"ResourceId": dirID, "Tags": tags})

		// First page: limit 2
		rec := doRequest(
			t,
			h,
			"ListTagsForResource",
			map[string]any{"ResourceId": dirID, "Limit": 2},
		)
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		firstPage, _ := body["Tags"].([]any)
		assert.Len(t, firstPage, 2)
		nextToken, _ := body["NextToken"].(string)
		assert.NotEmpty(t, nextToken)

		// Second page
		rec2 := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID, "Limit": 2, "NextToken": nextToken,
		})
		assert.Equal(t, http.StatusOK, rec2.Code)
		body2 := respBody(t, rec2)
		secondPage, _ := body2["Tags"].([]any)
		assert.Len(t, secondPage, 2)
		nextToken2, _ := body2["NextToken"].(string)
		assert.NotEmpty(t, nextToken2)

		// Third page (last)
		rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID, "Limit": 2, "NextToken": nextToken2,
		})
		assert.Equal(t, http.StatusOK, rec3.Code)
		body3 := respBody(t, rec3)
		thirdPage, _ := body3["Tags"].([]any)
		assert.Len(t, thirdPage, 1)
		_, hasMoreToken := body3["NextToken"]
		assert.False(t, hasMoreToken)
	})

	t.Run("no limit returns all tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		tags := make([]map[string]any, 10)
		for i := range 10 {
			tags[i] = map[string]any{"Key": fmt.Sprintf("k%02d", i), "Value": "v"}
		}
		doRequest(t, h, "AddTagsToResource", map[string]any{"ResourceId": dirID, "Tags": tags})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		result, _ := body["Tags"].([]any)
		assert.Len(t, result, 10)
		_, hasToken := body["NextToken"]
		assert.False(t, hasToken)
	})
}

func TestAddTagsToResource_UpsertSemantics(t *testing.T) {
	t.Parallel()

	t.Run("updating an existing key overwrites value", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
		})
		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "prod"}},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "prod", tags[0].(map[string]any)["Value"])
	})

	t.Run("multiple tags added in one call", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "a", "Value": "1"},
				{"Key": "b", "Value": "2"},
				{"Key": "c", "Value": "3"},
			},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		assert.Len(t, tags, 3)
	})

	t.Run("tags returned in sorted key order", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags": []map[string]any{
				{"Key": "zebra", "Value": "z"},
				{"Key": "apple", "Value": "a"},
				{"Key": "mango", "Value": "m"},
			},
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, rec)
		tags, _ := body["Tags"].([]any)
		require.Len(t, tags, 3)
		assert.Equal(t, "apple", tags[0].(map[string]any)["Key"])
		assert.Equal(t, "mango", tags[1].(map[string]any)["Key"])
		assert.Equal(t, "zebra", tags[2].(map[string]any)["Key"])
	})

	t.Run("RemoveTagsFromResource with non-existent key is idempotent", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
		})

		// Remove a key that doesn't exist — should succeed silently
		rec := doRequest(t, h, "RemoveTagsFromResource", map[string]any{
			"ResourceId": dirID,
			"TagKeys":    []string{"nonexistent"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		// Original tag still present
		listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": dirID})
		body := respBody(t, listRec)
		tags, _ := body["Tags"].([]any)
		assert.Len(t, tags, 1)
	})
}

// --- Conditional forwarder lifecycle ---
