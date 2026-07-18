package ram_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("tag-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/tagresource", map[string]any{
				"resourceShareArn": shareARN,
				"tags":             []map[string]string{{"key": "Env", "value": "test"}},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare(
					"untag-share",
					true,
					map[string]string{"Env": "test"},
					nil,
					nil,
				)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/untagresource", map[string]any{
				"resourceShareArn": shareARN,
				"tagKeys":          []string{"Env"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare(
					"listtag-share",
					true,
					map[string]string{"Env": "prod"},
					nil,
					nil,
				)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "Env",
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
				"resourceShareArn": shareARN,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_TagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/tagresource", map[string]any{
		"tags": []map[string]string{{"key": "Env", "value": "test"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/untagresource", map[string]any{
		"tagKeys": []string{"Env"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTagsForResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_ToTagObjectsSorted verifies tags are serialised in sorted order.
func TestToTagObjectsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name": "tagged-share",
		"tags": []map[string]string{
			{"key": "zz", "value": "last"},
			{"key": "aa", "value": "first"},
			{"key": "mm", "value": "middle"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List the share and verify the tags are in order.
	rec2 := doRAMRequest(t, h, "/getresourceshares", map[string]any{"resourceOwner": "SELF"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		ResourceShares []struct {
			Tags []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"tags"`
		} `json:"resourceShares"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.ResourceShares, 1)

	tags := resp.ResourceShares[0].Tags
	require.Len(t, tags, 3)
	assert.Equal(t, "aa", tags[0].Key)
	assert.Equal(t, "mm", tags[1].Key)
	assert.Equal(t, "zz", tags[2].Key)
}

// TestRefinement1_TagsRoundTrip verifies tag operations via HTTP.
func TestTagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("tag-rt-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Tag the share.
	rec := doRAMRequest(t, h, "/tagresource", map[string]any{
		"resourceShareArn": rs.ARN,
		"tags":             []map[string]string{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List tags.
	listRec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
		"resourceShareArn": rs.ARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Tags, 1)
	assert.Equal(t, "env", listResp.Tags[0].Key)
	assert.Equal(t, "prod", listResp.Tags[0].Value)

	// Untag.
	untagRec := doRAMRequest(t, h, "/untagresource", map[string]any{
		"resourceShareArn": rs.ARN,
		"tagKeys":          []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Tags should now be empty.
	listRec2 := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
		"resourceShareArn": rs.ARN,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Tags)
}
