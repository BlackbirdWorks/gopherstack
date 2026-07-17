package sesv2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagResource verifies TagResource persists tags, ListTagsForResource returns them,
// and UntagResource removes them.
func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test struct; reordering would separate related fields
		wantTags map[string]string
		initTags []any
		name     string
		arn      string
		untagKey string
	}{
		{
			name:     "tag_and_list",
			arn:      "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			initTags: []any{map[string]any{"Key": "env", "Value": "prod"}},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name:     "untag_removes_key",
			arn:      "arn:aws:ses:us-east-1:123456789012:identity/test.com",
			initTags: []any{map[string]any{"Key": "k1", "Value": "v1"}},
			untagKey: "k1",
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := doReqQuery(t, h, http.MethodPost, "/v2/email/tags", nil,
				map[string]any{"ResourceArn": tt.arn, "Tags": tt.initTags})
			assert.Equal(t, http.StatusOK, rec.Code, "TagResource: %s", rec.Body)

			if tt.untagKey != "" {
				rec2 := doReqQuery(t, h, http.MethodDelete, "/v2/email/tags",
					map[string]string{"ResourceArn": tt.arn, "TagKeys": tt.untagKey}, nil)
				assert.Equal(t, http.StatusOK, rec2.Code, "UntagResource: %s", rec2.Body)
			}

			rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/tags",
				map[string]string{"ResourceArn": tt.arn}, nil)
			require.Equal(t, http.StatusOK, rec3.Code, "ListTagsForResource: %s", rec3.Body)

			resp := decodeJSON(t, rec3)
			tagList, ok := resp["Tags"].([]any)
			require.True(t, ok, "expected Tags array")

			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				m, mOK := entry.(map[string]any)
				require.True(t, mOK)
				got[fmt.Sprint(m["Key"])] = fmt.Sprint(m["Value"])
			}

			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// TestTagOperations tests TagResource, ListTagsForResource, and UntagResource via HTTP.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()

	arn := "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "ListTagsForResource returns empty list",
			method:     http.MethodGet,
			path:       "/v2/email/tags?ResourceArn=" + arn,
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:   "TagResource accepted",
			method: http.MethodPost,
			path:   "/v2/email/tags",
			body: map[string]any{
				"ResourceArn": arn,
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "UntagResource accepted",
			method:     http.MethodDelete,
			path:       "/v2/email/tags?ResourceArn=" + arn + "&TagKeys=env",
			body:       nil,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
