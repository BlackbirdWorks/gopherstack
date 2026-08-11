package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "tags_stored_and_listed",
			body: map[string]any{
				"ResourceArn": "arn:aws:route53resolver:us-east-1:000000000000:resolver-endpoint/rslvr-in-aabbccdd",
				"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_body",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantTags int
	}{
		{
			name:     "returns_tags_after_tagging",
			wantCode: http.StatusOK,
			wantTags: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create an endpoint to get its ARN.
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "test-ep",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			ep := createResp["ResolverEndpoint"].(map[string]any)
			epARN := ep["Arn"].(string)

			// Tag the resource.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": epARN,
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// List tags.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": epARN,
			})
			assert.Equal(t, tt.wantCode, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			tags := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestListTagsForResource_Pagination verifies MaxResults/NextToken --
// verified against api_op_ListTagsForResource.go, which carries both on
// the request and the response ("If you have more than MaxResults tags,
// you can submit another ListTagsForResource request ... specify the
// value of NextToken from the previous response"). The wire struct
// previously had neither field, so every call returned the full unpaged
// list regardless of MaxResults.
func TestListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "paginated-tags-ep",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	epARN := createResp["ResolverEndpoint"].(map[string]any)["Arn"].(string)

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": epARN,
		"Tags": []map[string]string{
			{"Key": "a", "Value": "1"},
			{"Key": "b", "Value": "2"},
			{"Key": "c", "Value": "3"},
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	page1Rec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": epARN,
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, page1Rec.Code)
	var page1 map[string]any
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1))
	page1Tags := page1["Tags"].([]any)
	assert.Len(t, page1Tags, 2)
	nextToken, ok := page1["NextToken"].(string)
	require.True(t, ok, "a partial page must carry a NextToken")
	require.NotEmpty(t, nextToken)

	page2Rec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": epARN,
		"MaxResults":  2,
		"NextToken":   nextToken,
	})
	require.Equal(t, http.StatusOK, page2Rec.Code)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2))
	page2Tags := page2["Tags"].([]any)
	assert.Len(t, page2Tags, 1)
	_, hasNext := page2["NextToken"]
	assert.False(t, hasNext, "the final page must not carry a NextToken")
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		removeKey string
		wantCode  int
		wantTags  int
	}{
		{
			name:      "removes_tag",
			wantCode:  http.StatusOK,
			wantTags:  1,
			removeKey: "env",
		},
		{
			name:      "removes_nonexistent_key_is_ok",
			wantCode:  http.StatusOK,
			wantTags:  2,
			removeKey: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create an endpoint.
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "untag-ep",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			ep := createResp["ResolverEndpoint"].(map[string]any)
			epARN := ep["Arn"].(string)

			// Tag with two keys.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": epARN,
				"Tags": []map[string]string{
					{"Key": "env", "Value": "test"},
					{"Key": "team", "Value": "platform"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Untag.
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": epARN,
				"TagKeys":     []string{tt.removeKey},
			})
			assert.Equal(t, tt.wantCode, untagRec.Code)

			// List remaining tags.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": epARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			tags := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}
