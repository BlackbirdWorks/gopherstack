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
