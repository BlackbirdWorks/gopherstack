package codeartifact_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagsForDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain.
	createRec := doRequest(t, h, http.MethodPost, "/v1/domain?domain=tag-domain", map[string]any{
		"tags": []map[string]any{{"key": "env", "value": "dev"}},
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	domainMap, _ := createResp["domain"].(map[string]any)
	domainARN := domainMap["arn"].(string)

	// List tags.
	listRec := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tagList, _ := listResp["tags"].([]any)
	assert.Len(t, tagList, 1)

	// Tag resource.
	tagRec := doRequest(t, h, http.MethodPost, "/v1/tag?resourceArn="+domainARN, map[string]any{
		"tags": []map[string]any{{"key": "team", "value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags again - should have 2 now.
	listRec2 := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	tagList2, _ := listResp2["tags"].([]any)
	assert.Len(t, tagList2, 2)

	// Untag resource.
	untagRec := doRequest(t, h, http.MethodPost, "/v1/untag?resourceArn="+domainARN, map[string]any{
		"tagKeys": []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// List tags - should have 1 now.
	listRec3 := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec3.Code)

	var listResp3 map[string]any
	require.NoError(t, json.Unmarshal(listRec3.Body.Bytes(), &listResp3))
	tagList3, _ := listResp3["tags"].([]any)
	assert.Len(t, tagList3, 1)
}

func TestHandler_SortedTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/domain?domain=tag-sort-domain", map[string]any{
		"tags": []map[string]any{
			{"key": "z-key", "value": "z"},
			{"key": "a-key", "value": "a"},
			{"key": "m-key", "value": "m"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	domainMap, _ := createResp["domain"].(map[string]any)
	domainARN, _ := domainMap["arn"].(string)

	listRec := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tagList, _ := listResp["tags"].([]any)
	require.Len(t, tagList, 3)

	keys := make([]string, 3)
	for i, entry := range tagList {
		e, _ := entry.(map[string]any)
		keys[i], _ = e["key"].(string)
	}
	assert.Equal(t, []string{"a-key", "m-key", "z-key"}, keys)
}
