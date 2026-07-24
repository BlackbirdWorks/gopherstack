package inspector2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestTagsLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "tag_filter_and_list",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "tag-target")

				tagRec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
					"tags": map[string]string{
						"team": "security",
						"env":  "staging",
					},
				})
				require.Equal(t, http.StatusOK, tagRec.Code, tagRec.Body.String())

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "security", tags["team"])
				assert.Equal(t, "staging", tags["env"])
			},
		},
		{
			name: "untag_removes_specific_keys",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "untag-target")

				auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
					"tags": map[string]string{
						"keep":   "yes",
						"remove": "no",
					},
				})

				// DELETE with query params
				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, "/tags/"+filterARN+"?tagKeys=remove", nil)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				c.SetRequest(req)
				require.NoError(t, h.Handler()(c))
				require.Equal(t, http.StatusOK, rec.Code)

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.Equal(t, "yes", tags["keep"])
				_, hasRemove := tags["remove"]
				assert.False(t, hasRemove)
			},
		},
		{
			name: "tag_nonexistent_resource_returns_404",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(
					t,
					h,
					http.MethodPost,
					"/tags/arn:aws:inspector2:us-east-1:123456789012:filter/nonexistent",
					map[string]any{
						"tags": map[string]string{"k": "v"},
					},
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_tags_nonexistent_resource_returns_404",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(
					t,
					h,
					http.MethodGet,
					"/tags/arn:aws:inspector2:us-east-1:123456789012:filter/ghost",
					nil,
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_tags_empty_on_new_filter",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				filterARN := auditCreateFilter(t, h, "no-tags")

				listRec := auditDo(t, h, http.MethodGet, "/tags/"+filterARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.Empty(t, tags)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

func TestTagResourceKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     map[string]string{"env": "prod", "team": "security"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key at limit accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over limit rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at limit accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over limit rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			filterARN := auditCreateFilter(t, h, "tag-validation-"+sanitizeFilterName(tc.name))

			rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
				"tags": tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestTagResourceCountLimit(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	filterARN := auditCreateFilter(t, h, "tag-count-filter")

	bulk := make(map[string]string, 50)
	for i := range 50 {
		bulk[strings.Repeat("k", 1)+string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}

	rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{"tags": bulk})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
		"tags": map[string]string{"overflow-key": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTagResourceInvalidKeyErrorType(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	filterARN := auditCreateFilter(t, h, "err-type-filter")

	rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
		"tags": map[string]string{"": "value"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, inspector2UnmarshalError(t, rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}
