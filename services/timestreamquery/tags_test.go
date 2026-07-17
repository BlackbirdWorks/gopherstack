package timestreamquery_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestreamQueryHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		sqName   string
		wantCode int
	}{
		{
			name:     "tag resource",
			sqName:   "tagged-query",
			tags:     map[string]string{"env": "test", "team": "data"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			// Create a scheduled query with create-time tags to get an ARN
			createBody := map[string]any{
				"Name":                           tt.sqName,
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "bucket"},
				},
				"Tags": []map[string]string{
					{"Key": "created-by", "Value": "test"},
				},
			}

			rec := doRequest(t, h, "CreateScheduledQuery", createBody)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			arn := resp["Arn"].(string)

			// Create-time tags must be visible via ListTagsForResource.
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			createTimeTags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, createTimeTags, 1, "create-time tags must be returned by ListTagsForResource")

			// TagResource
			tagItems := make([]map[string]string, 0, len(tt.tags))
			for k, v := range tt.tags {
				tagItems = append(tagItems, map[string]string{"Key": k, "Value": v})
			}

			rec = doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tagItems,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			// ListTagsForResource — should return create-time tag + added tags.
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, len(tt.tags)+1) // create-time tag plus added tags

			// UntagResource
			keys := make([]string, 0, len(tt.tags))
			for k := range tt.tags {
				keys = append(keys, k)
			}

			rec = doRequest(t, h, "UntagResource", map[string]any{
				"ResourceARN": arn,
				"TagKeys":     keys,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// ListTagsForResource after untag — only create-time tag remains.
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp = parseResponse(t, rec)
			tags, ok = resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, 1, "only create-time tag should remain after untag")
		})
	}
}

func TestTimestreamQueryHandler_TagValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "tag - missing arn",
			op:       "TagResource",
			body:     map[string]any{"Tags": []any{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "untag - missing arn",
			op:       "UntagResource",
			body:     map[string]any{"TagKeys": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list tags - missing arn",
			op:       "ListTagsForResource",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestTimestreamQueryHandler_TagResourceNotFound verifies TagResource on a
// non-existent ARN returns a not-found error.
func TestTimestreamQueryHandler_TagResourceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "TagResource on non-existent ARN returns not found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": "arn:aws:timestream:us-east-1:123:scheduled-query/nope",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
