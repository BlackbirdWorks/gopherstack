package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func TestAppStream_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler) string
		check    func(t *testing.T, body []byte)
		body     func(arn string) any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "TagResource applies tags to stack",
			action: "TagResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "tag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"tag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "prod"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			action: "ListTagsForResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "listtag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"listtag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
				doRequest(
					t,
					h,
					"TagResource",
					map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "staging"}},
				)

				return arn
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "staging", tags["env"])
			},
		},
		{
			name:   "UntagResource removes tags",
			action: "UntagResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "untag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"untag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
				doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v"}})

				return arn
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "TagKeys": []string{"k"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource unknown ARN returns error",
			action: "ListTagsForResource",
			setup:  func(_ *appstream.Handler) string { return "" },
			body: func(_ string) any {
				return map[string]any{"ResourceArn": "arn:aws:appstream:us-east-1:000000000000:stack/no-such"}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := tc.setup(h)
			rec := doRequest(t, h, tc.action, tc.body(arn))
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_TagLifecycle verifies the tag lifecycle: tag, list, untag.
func TestAppStream_TagLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStack", map[string]any{
		"Name": "tagged-stack",
		"Tags": map[string]any{"env": "test", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	stackArn := createResp["Stack"].(map[string]any)["Arn"].(string)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": stackArn})
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": stackArn,
		"TagKeys":     []string{"env"},
	})

	recList2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": stackArn})
	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp2))
	tags2 := listResp2["Tags"].(map[string]any)
	assert.NotContains(t, tags2, "env")
	assert.Equal(t, "platform", tags2["team"])
}
