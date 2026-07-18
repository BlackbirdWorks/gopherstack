package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- Tags ---

func TestWorkMail_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "tag_and_list_tags",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-abc123"
				rec := doOp(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"prod"},{"Key":"Team","Value":"eng"}]}`, arn,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				require.Equal(t, http.StatusOK, rec2.Code)
				m := decodeJSON(t, rec2)
				tags, ok := m["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 2)
				keys := make([]string, 0, len(tags))
				for _, t := range tags {
					tag := t.(map[string]any)
					keys = append(keys, tag["Key"].(string))
				}
				assert.Contains(t, keys, "Env")
				assert.Contains(t, keys, "Team")
			},
		},
		{
			name: "untag_removes_key",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-def456"
				doOp(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"A","Value":"1"},{"Key":"B","Value":"2"}]}`, arn,
				))
				rec := doOp(t, h, "UntagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"TagKeys":["A"]}`, arn,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doOp(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				m := decodeJSON(t, rec2)
				tags := m["Tags"].([]any)
				assert.Len(t, tags, 1)
				assert.Equal(t, "B", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tag_overwrite_same_key",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				arn := "arn:aws:workmail:us-east-1:000000000000:organization/m-ghi789"
				doOp(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"dev"}]}`, arn,
				))
				doOp(t, h, "TagResource", fmt.Sprintf(
					`{"ResourceARN":%q,"Tags":[{"Key":"Env","Value":"prod"}]}`, arn,
				))
				rec := doOp(t, h, "ListTagsForResource", fmt.Sprintf(`{"ResourceARN":%q}`, arn))
				m := decodeJSON(t, rec)
				tags := m["Tags"].([]any)
				assert.Len(t, tags, 1)
				assert.Equal(t, "prod", tags[0].(map[string]any)["Value"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.run(t, h)
		})
	}
}
