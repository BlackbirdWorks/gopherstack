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

// ---- Trust Anchor HTTP CRUD ----

func TestHandler_TrustAnchor_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		name       string
		wantCreate int
		wantGet    int
	}{
		{
			name: "create and get trust anchor",
			createBody: map[string]any{
				"name":   "anchor-http-test",
				"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
			},
			wantCreate: http.StatusCreated,
			wantGet:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doREST(t, h, http.MethodPost, "/trustanchors", tt.createBody)
			assert.Equal(t, tt.wantCreate, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			ta := createResp["trustAnchor"].(map[string]any)
			id := ta["trustAnchorId"].(string)
			assert.NotEmpty(t, id)

			// Get.
			recGet := doREST(t, h, http.MethodGet, "/trustanchor/"+id, nil)
			assert.Equal(t, tt.wantGet, recGet.Code)

			// List.
			recList := doREST(t, h, http.MethodGet, "/trustanchors", nil)
			assert.Equal(t, http.StatusOK, recList.Code)

			// Update.
			recUpdate := doREST(
				t,
				h,
				http.MethodPatch,
				"/trustanchor/"+id,
				map[string]any{"name": "renamed-anchor"},
			)
			assert.Equal(t, http.StatusOK, recUpdate.Code)

			// Enable / Disable.
			recDisable := doREST(t, h, http.MethodPost, "/trustanchor/"+id+"/disable", nil)
			assert.Equal(t, http.StatusOK, recDisable.Code)

			recEnable := doREST(t, h, http.MethodPost, "/trustanchor/"+id+"/enable", nil)
			assert.Equal(t, http.StatusOK, recEnable.Code)

			// Delete.
			recDelete := doREST(t, h, http.MethodDelete, "/trustanchor/"+id, nil)
			assert.Equal(t, http.StatusOK, recDelete.Code)

			// Get after delete → 404.
			recGetGone := doREST(t, h, http.MethodGet, "/trustanchor/"+id, nil)
			assert.Equal(t, http.StatusNotFound, recGetGone.Code)
		})
	}
}

func TestHandler_TrustAnchor_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		raw        []byte
		wantStatus int
	}{
		{
			name:       "create trust anchor with invalid json",
			path:       "/trustanchors",
			method:     http.MethodPost,
			raw:        []byte(`{invalid`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update trust anchor with invalid json",
			path:       "/trustanchor/some-id",
			method:     http.MethodPatch,
			raw:        []byte(`{invalid`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(tt.raw))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TrustAnchor_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "get nonexistent → 404",
			method:     http.MethodGet,
			path:       "/trustanchor/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete nonexistent → 404",
			method:     http.MethodDelete,
			path:       "/trustanchor/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "enable nonexistent → 404",
			method:     http.MethodPost,
			path:       "/trustanchor/no-such-id/enable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "disable nonexistent → 404",
			method:     http.MethodPost,
			path:       "/trustanchor/no-such-id/disable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update nonexistent → 404",
			method:     http.MethodPatch,
			path:       "/trustanchor/no-such-id",
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TrustAnchor_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"duplicate name returns conflict", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name":   "dup-anchor-http",
				"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
			}
			doREST(t, h, http.MethodPost, "/trustanchors", body)
			rec := doREST(t, h, http.MethodPost, "/trustanchors", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- List TrustAnchors with pagination ----

func TestHandler_ListTrustAnchors_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantStatus int
		wantCount  int
	}{
		{"no pagination returns all", "", http.StatusOK, 3},
		{"maxResults=1 returns 1", "?maxResults=1", http.StatusOK, 1},
		{"maxResults=2 returns 2", "?maxResults=2", http.StatusOK, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range 3 {
				doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
					"name":   "anchor-page-" + string(rune('a'+i)),
					"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
				})
			}
			rec := doREST(t, h, http.MethodGet, "/trustanchors"+tt.query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items := resp["trustAnchors"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}
