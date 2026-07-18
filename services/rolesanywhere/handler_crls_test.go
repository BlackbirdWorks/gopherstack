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

// ---- CRL HTTP CRUD ----

func TestHandler_CRL_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		name       string
		wantCreate int
	}{
		{
			name: "import and full CRL lifecycle",
			createBody: map[string]any{
				"name":           "http-crl",
				"crlData":        []byte("ZmFrZWRhdGE="),
				"trustAnchorArn": "arn:aws:rolesanywhere:us-east-1:123:trust-anchor/ta",
			},
			wantCreate: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Import.
			rec := doREST(t, h, http.MethodPost, "/crls", tt.createBody)
			assert.Equal(t, tt.wantCreate, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			crl := createResp["crl"].(map[string]any)
			id := crl["crlId"].(string)
			assert.NotEmpty(t, id)

			// Get.
			recGet := doREST(t, h, http.MethodGet, "/crl/"+id, nil)
			assert.Equal(t, http.StatusOK, recGet.Code)

			// List.
			recList := doREST(t, h, http.MethodGet, "/crls", nil)
			assert.Equal(t, http.StatusOK, recList.Code)

			// Update.
			recUpdate := doREST(
				t,
				h,
				http.MethodPatch,
				"/crl/"+id,
				map[string]any{"name": "updated-crl"},
			)
			assert.Equal(t, http.StatusOK, recUpdate.Code)

			// Disable / Enable.
			recDisable := doREST(t, h, http.MethodPost, "/crl/"+id+"/disable", nil)
			assert.Equal(t, http.StatusOK, recDisable.Code)

			recEnable := doREST(t, h, http.MethodPost, "/crl/"+id+"/enable", nil)
			assert.Equal(t, http.StatusOK, recEnable.Code)

			// Delete.
			recDelete := doREST(t, h, http.MethodDelete, "/crl/"+id, nil)
			assert.Equal(t, http.StatusOK, recDelete.Code)

			// Get after delete → 404.
			recGetGone := doREST(t, h, http.MethodGet, "/crl/"+id, nil)
			assert.Equal(t, http.StatusNotFound, recGetGone.Code)
		})
	}
}

func TestHandler_CRL_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "get nonexistent crl → 404",
			method:     http.MethodGet,
			path:       "/crl/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete nonexistent crl → 404",
			method:     http.MethodDelete,
			path:       "/crl/no-such-id",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "enable nonexistent crl → 404",
			method:     http.MethodPost,
			path:       "/crl/no-such-id/enable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "disable nonexistent crl → 404",
			method:     http.MethodPost,
			path:       "/crl/no-such-id/disable",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update nonexistent crl → 404",
			method:     http.MethodPatch,
			path:       "/crl/no-such-id",
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

func TestHandler_CRL_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		wantStatus int
	}{
		{"import crl with invalid json", "/crls", http.MethodPost, http.StatusBadRequest},
		{"update crl with invalid json", "/crl/some-id", http.MethodPatch, http.StatusBadRequest},
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

func TestHandler_CRL_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"duplicate crl name returns conflict", http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name":           "dup-crl-http",
				"trustAnchorArn": "arn:aws:ta",
			}
			doREST(t, h, http.MethodPost, "/crls", body)
			rec := doREST(t, h, http.MethodPost, "/crls", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
