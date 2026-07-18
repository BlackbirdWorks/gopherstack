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

// ---- Profile HTTP CRUD ----

func TestHandler_Profile_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		name       string
		wantCreate int
	}{
		{
			name: "create and full lifecycle",
			createBody: map[string]any{
				"name":     "http-profile",
				"roleArns": []string{"arn:aws:iam::123456789012:role/MyRole"},
			},
			wantCreate: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doREST(t, h, http.MethodPost, "/profiles", tt.createBody)
			assert.Equal(t, tt.wantCreate, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			p := createResp["profile"].(map[string]any)
			id := p["profileId"].(string)
			assert.NotEmpty(t, id)

			// Get.
			recGet := doREST(t, h, http.MethodGet, "/profile/"+id, nil)
			assert.Equal(t, http.StatusOK, recGet.Code)

			// List.
			recList := doREST(t, h, http.MethodGet, "/profiles", nil)
			assert.Equal(t, http.StatusOK, recList.Code)

			// Update.
			dur := int32(3600)
			recUpdate := doREST(t, h, http.MethodPatch, "/profile/"+id, map[string]any{
				"name":            "renamed-profile",
				"durationSeconds": dur,
				"sessionPolicy":   "{}",
			})
			assert.Equal(t, http.StatusOK, recUpdate.Code)

			// Disable / Enable.
			recDisable := doREST(t, h, http.MethodPost, "/profile/"+id+"/disable", nil)
			assert.Equal(t, http.StatusOK, recDisable.Code)

			recEnable := doREST(t, h, http.MethodPost, "/profile/"+id+"/enable", nil)
			assert.Equal(t, http.StatusOK, recEnable.Code)

			// Delete.
			recDelete := doREST(t, h, http.MethodDelete, "/profile/"+id, nil)
			assert.Equal(t, http.StatusOK, recDelete.Code)

			// Get after delete → 404.
			recGetGone := doREST(t, h, http.MethodGet, "/profile/"+id, nil)
			assert.Equal(t, http.StatusNotFound, recGetGone.Code)
		})
	}
}

func TestHandler_Profile_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "get nonexistent profile → 404",
			method:     http.MethodGet,
			path:       "/profile/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete nonexistent profile → 404",
			method:     http.MethodDelete,
			path:       "/profile/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "enable nonexistent profile → 404",
			method:     http.MethodPost,
			path:       "/profile/no-such-id/enable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "disable nonexistent profile → 404",
			method:     http.MethodPost,
			path:       "/profile/no-such-id/disable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update nonexistent profile → 404",
			method:     http.MethodPatch,
			path:       "/profile/no-such-id",
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

func TestHandler_Profile_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		wantStatus int
	}{
		{"create profile with invalid json", "/profiles", http.MethodPost, http.StatusBadRequest},
		{
			"update profile with invalid json",
			"/profile/some-id",
			http.MethodPatch,
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader([]byte(`{invalid`)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Profile_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"duplicate profile name returns conflict", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"name": "dup-http-profile", "roleArns": []string{}}
			doREST(t, h, http.MethodPost, "/profiles", body)
			rec := doREST(t, h, http.MethodPost, "/profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
