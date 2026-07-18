package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	const primaryWGARN = "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"ResourceARN":"` + primaryWGARN + `","Tags":[{"Key":"env","Value":"prod"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

			_ = doRequest(t, h, "TagResource",
				`{"ResourceARN":"`+arn+`","Tags":[{"Key":"env","Value":"prod"}]}`)

			rec := doRequest(t, h, "UntagResource",
				`{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_tags_after_tagging",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

			_ = doRequest(t, h, "TagResource",
				`{"ResourceARN":"`+arn+`","Tags":[{"Key":"team","Value":"platform"}]}`)

			rec := doRequest(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string][]map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["Tags"], tt.wantCount)
			assert.Equal(t, "team", resp["Tags"][0]["Key"])
			assert.Equal(t, "platform", resp["Tags"][0]["Value"])
		})
	}
}

// --- Unknown operation ---

func TestTag_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "tag_list_untag_workgroup",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				const arn = "arn:aws:athena:us-east-1:000000000000:workgroup/primary"

				rec := a1Do(t, h, "TagResource",
					`{"ResourceARN":"`+arn+`","Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"data"}]}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				tags, ok := m["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 2)

				// Verify tag shape: Key and Value fields.
				for _, item := range tags {
					tag := item.(map[string]any)
					assert.NotEmpty(t, tag["Key"])
					assert.NotEmpty(t, tag["Value"])
				}

				rec = a1Do(t, h, "UntagResource",
					`{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				tags2, _ := a1Unmarshal(t, rec)["Tags"].([]any)
				assert.Len(t, tags2, 1)
				remaining := tags2[0].(map[string]any)
				assert.Equal(t, "team", remaining["Key"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := a1Handler(t)
			tt.fn(t, h)
		})
	}
}
