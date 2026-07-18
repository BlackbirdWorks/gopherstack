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

// ---- Attribute Mapping HTTP ----

func TestHandler_AttributeMapping_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"put and delete attribute mapping", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create profile first.
			recProfile := doREST(t, h, http.MethodPost, "/profiles", map[string]any{
				"name":     "mapping-http-profile",
				"roleArns": []string{"arn:aws:iam::123:role/R"},
			})
			require.Equal(t, http.StatusCreated, recProfile.Code)

			var profileResp map[string]any
			require.NoError(t, json.Unmarshal(recProfile.Body.Bytes(), &profileResp))
			p := profileResp["profile"].(map[string]any)
			profileID := p["profileId"].(string)

			// Put mapping.
			recPut := doREST(
				t,
				h,
				http.MethodPut,
				"/profiles/"+profileID+"/mappings",
				map[string]any{
					"certificateField": "x509Subject",
					"mappingRules":     []map[string]any{{"specifier": "CN"}},
				},
			)
			assert.Equal(t, tt.wantStatus, recPut.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putResp))
			profileInResp := putResp["profile"].(map[string]any)
			assert.Equal(t, profileID, profileInResp["profileId"])

			// Delete mapping.
			recDelete := doREST(t, h, http.MethodDelete,
				"/profiles/"+profileID+"/mappings?certificateField=x509Subject", nil)
			assert.Equal(t, http.StatusOK, recDelete.Code)
		})
	}
}

func TestHandler_AttributeMapping_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"put mapping invalid json → 400", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPut,
				"/profiles/some-id/mappings",
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

func TestHandler_AttributeMapping_ProfileNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "put mapping nonexistent profile → 404",
			method:     http.MethodPut,
			path:       "/profiles/no-such-profile/mappings",
			body:       map[string]any{"certificateField": "x509Subject", "mappingRules": []any{}},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete mapping nonexistent profile → 404",
			method:     http.MethodDelete,
			path:       "/profiles/no-such-profile/mappings?certificateField=x509Subject",
			body:       nil,
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
