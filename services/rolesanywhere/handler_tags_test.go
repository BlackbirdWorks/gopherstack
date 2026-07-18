package rolesanywhere_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Tag HTTP operations ----

func TestHandler_Tags_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"tag, list, untag resource", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resARN := "arn:aws:rolesanywhere:us-east-1:000000000000:trust-anchor/tag-test-id"

			// Tag resource.
			recTag := doREST(t, h, http.MethodPost, "/TagResource", map[string]any{
				"resourceArn": resARN,
				"tags": []map[string]any{
					{"key": "env", "value": "test"},
					{"key": "team", "value": "security"},
				},
			})
			assert.Equal(t, tt.wantStatus, recTag.Code)

			// List tags.
			recList := doREST(t, h, http.MethodGet, "/ListTagsForResource?resourceArn="+resARN, nil)
			assert.Equal(t, http.StatusOK, recList.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
			tags := listResp["tags"].([]any)
			assert.Len(t, tags, 2)

			// Untag resource. AWS sends resourceArn/tagKeys as a JSON body on
			// POST /UntagResource, not as query params.
			recUntag := doREST(
				t,
				h,
				http.MethodPost,
				"/UntagResource",
				map[string]any{
					"resourceArn": resARN,
					"tagKeys":     []string{"env"},
				},
			)
			assert.Equal(t, http.StatusOK, recUntag.Code)

			// Verify tag removed.
			recList2 := doREST(
				t,
				h,
				http.MethodGet,
				"/ListTagsForResource?resourceArn="+resARN,
				nil,
			)
			require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp))
			tags2 := listResp["tags"].([]any)
			assert.Len(t, tags2, 1)
		})
	}
}

func TestHandler_TagResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"tag resource invalid json → 400", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/TagResource",
				bytes.NewReader([]byte(`{invalid`)),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Tags_EmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantTags   int
	}{
		{"list tags for unknown resource returns empty list", http.StatusOK, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(
				t,
				h,
				http.MethodGet,
				"/ListTagsForResource?resourceArn=arn:aws:unknown",
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tags := resp["tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}
