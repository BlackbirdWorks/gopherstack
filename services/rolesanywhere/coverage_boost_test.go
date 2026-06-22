package rolesanywhere_test

// coverage_boost_test.go adds table-driven tests targeting handler, provider,
// and backend paths at 0% coverage to push total coverage above 90%.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- test helpers ----

func newTestHandler(t *testing.T) *rolesanywhere.Handler {
	t.Helper()
	b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")

	return rolesanywhere.NewHandler(b)
}

// doREST sends an HTTP request to the handler and returns the response recorder.
func doREST(
	t *testing.T,
	h *rolesanywhere.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- Handler metadata ----

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "RolesAnywhere", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// create a trust anchor via the handler, then reset
	rec := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
		"name":   "reset-test",
		"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	// after reset, list should return empty
	rec2 := doREST(t, h, http.MethodGet, "/trustanchors", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	items, _ := resp["trustAnchors"].([]any)
	assert.Empty(t, items)
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateTrustAnchor")
	assert.Contains(t, ops, "ImportCrl")
	assert.Contains(t, ops, "TagResource")
}

// ---- RouteMatcher ----

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{"trustanchors collection", "/trustanchors", true},
		{"trustanchor by id", "/trustanchor/some-id", true},
		{"profiles collection", "/profiles", true},
		{"profile by id", "/profile/some-id", true},
		{"crls collection", "/crls", true},
		{"crl by id", "/crl/some-id", true},
		{"subjects collection", "/subjects", true},
		{"subject by id", "/subject/some-id", true},
		{"put-notifications-settings", "/put-notifications-settings", true},
		{"reset-notifications-settings", "/reset-notifications-settings", true},
		{"TagResource", "/TagResource", true},
		{"UntagResource", "/UntagResource", true},
		{"ListTagsForResource", "/ListTagsForResource", true},
		{"unknown path", "/something-else", false},
		{"ec2 path", "/instances", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

// ---- ExtractOperation / ExtractResource ----

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			"GET /trustanchors → ListTrustAnchors",
			http.MethodGet,
			"/trustanchors",
			"ListTrustAnchors",
		},
		{
			"POST /trustanchors → CreateTrustAnchor",
			http.MethodPost,
			"/trustanchors",
			"CreateTrustAnchor",
		},
		{
			"GET /trustanchor/id → GetTrustAnchor",
			http.MethodGet,
			"/trustanchor/abc",
			"GetTrustAnchor",
		},
		{
			"DELETE /trustanchor/id → DeleteTrustAnchor",
			http.MethodDelete,
			"/trustanchor/abc",
			"DeleteTrustAnchor",
		},
		{
			"PATCH /trustanchor/id → UpdateTrustAnchor",
			http.MethodPatch,
			"/trustanchor/abc",
			"UpdateTrustAnchor",
		},
		{
			"POST /trustanchor/id/enable → EnableTrustAnchor",
			http.MethodPost,
			"/trustanchor/abc/enable",
			"EnableTrustAnchor",
		},
		{
			"POST /trustanchor/id/disable → DisableTrustAnchor",
			http.MethodPost,
			"/trustanchor/abc/disable",
			"DisableTrustAnchor",
		},
		{"GET /profiles → ListProfiles", http.MethodGet, "/profiles", "ListProfiles"},
		{"POST /profiles → CreateProfile", http.MethodPost, "/profiles", "CreateProfile"},
		{"GET /profile/id → GetProfile", http.MethodGet, "/profile/abc", "GetProfile"},
		{"DELETE /profile/id → DeleteProfile", http.MethodDelete, "/profile/abc", "DeleteProfile"},
		{"PATCH /profile/id → UpdateProfile", http.MethodPatch, "/profile/abc", "UpdateProfile"},
		{
			"POST /profile/id/enable → EnableProfile",
			http.MethodPost,
			"/profile/abc/enable",
			"EnableProfile",
		},
		{
			"POST /profile/id/disable → DisableProfile",
			http.MethodPost,
			"/profile/abc/disable",
			"DisableProfile",
		},
		{"GET /crls → ListCrls", http.MethodGet, "/crls", "ListCrls"},
		{"POST /crls → ImportCrl", http.MethodPost, "/crls", "ImportCrl"},
		{"GET /crl/id → GetCrl", http.MethodGet, "/crl/abc", "GetCrl"},
		{"DELETE /crl/id → DeleteCrl", http.MethodDelete, "/crl/abc", "DeleteCrl"},
		{"PATCH /crl/id → UpdateCrl", http.MethodPatch, "/crl/abc", "UpdateCrl"},
		{"POST /crl/id/enable → EnableCrl", http.MethodPost, "/crl/abc/enable", "EnableCrl"},
		{"POST /crl/id/disable → DisableCrl", http.MethodPost, "/crl/abc/disable", "DisableCrl"},
		{"GET /subjects → ListSubjects", http.MethodGet, "/subjects", "ListSubjects"},
		{"GET /subject/id → GetSubject", http.MethodGet, "/subject/abc", "GetSubject"},
		{
			"PUT /profiles/id/mappings → PutAttributeMapping",
			http.MethodPut,
			"/profiles/abc/mappings",
			"PutAttributeMapping",
		},
		{
			"DELETE /profiles/id/mappings → DeleteAttributeMapping",
			http.MethodDelete,
			"/profiles/abc/mappings",
			"DeleteAttributeMapping",
		},
		{
			"PATCH /put-notifications-settings → PutNotificationSettings",
			http.MethodPatch,
			"/put-notifications-settings",
			"PutNotificationSettings",
		},
		{
			"PATCH /reset-notifications-settings → ResetNotificationSettings",
			http.MethodPatch,
			"/reset-notifications-settings",
			"ResetNotificationSettings",
		},
		{"POST /TagResource → TagResource", http.MethodPost, "/TagResource", "TagResource"},
		{"POST /UntagResource → UntagResource", http.MethodPost, "/UntagResource", "UntagResource"},
		{
			"GET /ListTagsForResource → ListTagsForResource",
			http.MethodGet,
			"/ListTagsForResource",
			"ListTagsForResource",
		},
		{"unknown path → Unknown", http.MethodGet, "/unknown-path", "Unknown"},
		{"wrong method on TagResource → Unknown", http.MethodGet, "/TagResource", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{"trustanchor by id returns id", http.MethodGet, "/trustanchor/my-id", "my-id"},
		{"trustanchors collection returns empty", http.MethodGet, "/trustanchors", ""},
		{"profile by id returns id", http.MethodGet, "/profile/p-id", "p-id"},
		{"crl by id returns id", http.MethodGet, "/crl/c-id", "c-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			res := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, res)
		})
	}
}

// ---- Unknown operation → 404 ----

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{"unknown path returns 404", http.MethodGet, "/unknown-path-xyz", http.StatusNotFound},
		{"wrong method on collection", http.MethodDelete, "/trustanchors", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

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

// ---- Subject HTTP ----

func TestHandler_Subject_GetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{"get nonexistent subject → 404", "/subject/no-such-subject", http.StatusNotFound},
		{"list subjects empty → 200", "/subjects", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

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

// ---- Notification Settings HTTP ----

func TestHandler_NotificationSettings_PutReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"put and reset notification settings", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create trust anchor first.
			recTA := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
				"name":   "notif-http-anchor",
				"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
			})
			require.Equal(t, http.StatusCreated, recTA.Code)

			var taResp map[string]any
			require.NoError(t, json.Unmarshal(recTA.Body.Bytes(), &taResp))
			ta := taResp["trustAnchor"].(map[string]any)
			taID := ta["trustAnchorId"].(string)

			// Put notification settings.
			recPut := doREST(t, h, http.MethodPatch, "/put-notifications-settings", map[string]any{
				"trustAnchorId": taID,
				"notificationSettings": []map[string]any{
					{"event": "CA_CERTIFICATE_EXPIRY", "enabled": true},
				},
			})
			assert.Equal(t, tt.wantStatus, recPut.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putResp))
			assert.Contains(t, putResp, "trustAnchor")

			// Reset notification settings.
			recReset := doREST(
				t,
				h,
				http.MethodPatch,
				"/reset-notifications-settings",
				map[string]any{
					"trustAnchorId": taID,
					"notificationSettingKeys": []map[string]any{
						{"event": "CA_CERTIFICATE_EXPIRY"},
					},
				},
			)
			assert.Equal(t, tt.wantStatus, recReset.Code)
		})
	}
}

func TestHandler_NotificationSettings_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			"put notifications invalid json → 400",
			"/put-notifications-settings",
			http.StatusBadRequest,
		},
		{
			"reset notifications invalid json → 400",
			"/reset-notifications-settings",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPatch,
				tt.path,
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

func TestHandler_NotificationSettings_TrustAnchorNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "put notifications nonexistent anchor → 404",
			path: "/put-notifications-settings",
			body: map[string]any{
				"trustAnchorId":        "no-such-anchor",
				"notificationSettings": []any{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "reset notifications nonexistent anchor → 404",
			path: "/reset-notifications-settings",
			body: map[string]any{
				"trustAnchorId":           "no-such-anchor",
				"notificationSettingKeys": []any{},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, http.MethodPatch, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

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

			// Untag resource.
			recUntag := doREST(
				t,
				h,
				http.MethodPost,
				"/UntagResource?resourceArn="+resARN+"&tagKeys=env",
				nil,
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

// ---- Backend: Region / AccountID / Snapshot / Restore ----

func TestBackend_RegionAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{"us-east-1", "111111111111", "us-east-1"},
		{"eu-west-1", "222222222222", "eu-west-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rolesanywhere.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.region, b.Region())
			assert.Equal(t, tt.accountID, b.AccountID())
		})
	}
}

func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		anchorName        string
		profileName       string
		expectAnchorCount int
		expectProfileCnt  int
	}{
		{
			name:              "snapshot and restore preserves state",
			anchorName:        "snap-anchor",
			profileName:       "snap-profile",
			expectAnchorCount: 1,
			expectProfileCnt:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
			_, err := b.CreateTrustAnchor(context.Background(), tt.anchorName, src, nil)
			require.NoError(t, err)
			_, err = b.CreateProfile(context.Background(), tt.profileName, nil, nil, nil, nil, "", false)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			assert.NotEmpty(t, snap)

			// Restore into a fresh backend.
			b2 := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			anchors, _, err := b2.ListTrustAnchors(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, anchors, tt.expectAnchorCount)

			profiles, _, err := b2.ListProfiles(context.Background(), "", 0)
			require.NoError(t, err)
			assert.Len(t, profiles, tt.expectProfileCnt)
		})
	}
}

func TestBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"invalid json returns error", []byte(`{invalid`), true},
		{"empty object restores cleanly", []byte(`{}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tt.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---- Backend: CreateTrustAnchor empty name validation ----

func TestBackend_CreateTrustAnchor_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		taName  string
		wantErr bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-name", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			src := rolesanywhere.TrustAnchorSource{SourceType: "CERTIFICATE_BUNDLE"}
			_, err := b.CreateTrustAnchor(context.Background(), tt.taName, src, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_CreateProfile_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantErr     bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-profile", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateProfile(context.Background(), tt.profileName, nil, nil, nil, nil, "", false)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackend_ImportCrl_EmptyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		crlName string
		wantErr bool
	}{
		{"empty name returns validation error", "", true},
		{"non-empty name succeeds", "valid-crl", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.ImportCrl(context.Background(), tt.crlName, []byte("data"), "arn:ta", true, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---- Backend: TagResource upsert behavior ----

func TestBackend_TagResource_Upsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValues map[string]string
		name       string
		initial    []rolesanywhere.TagEntry
		updates    []rolesanywhere.TagEntry
	}{
		{
			name: "update existing key preserves other keys",
			initial: []rolesanywhere.TagEntry{
				{Key: "env", Value: "dev"},
				{Key: "team", Value: "ops"},
			},
			updates: []rolesanywhere.TagEntry{{Key: "env", Value: "prod"}},
			wantValues: map[string]string{
				"env":  "prod",
				"team": "ops",
			},
		},
		{
			name:    "add new key",
			initial: []rolesanywhere.TagEntry{{Key: "a", Value: "1"}},
			updates: []rolesanywhere.TagEntry{{Key: "b", Value: "2"}},
			wantValues: map[string]string{
				"a": "1",
				"b": "2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			arn := "arn:aws:test::" + tt.name
			require.NoError(t, b.TagResource(context.Background(), arn, tt.initial))
			require.NoError(t, b.TagResource(context.Background(), arn, tt.updates))

			got, err := b.ListTagsForResource(context.Background(), arn)
			require.NoError(t, err)

			found := make(map[string]string)
			for _, tg := range got {
				found[tg.Key] = tg.Value
			}
			assert.Equal(t, tt.wantValues, found)
		})
	}
}

// ---- Backend: UpdateProfile all fields ----

func TestBackend_UpdateProfile_AllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		requireIP         *bool
		dur               *int32
		name              string
		newName           string
		sessionPolicy     string
		roleArns          []string
		managedPolicyArns []string
	}{
		{
			name:              "update all fields",
			newName:           "updated-profile",
			roleArns:          []string{"arn:aws:iam::123:role/NewRole"},
			managedPolicyArns: []string{"arn:aws:iam::123:policy/Managed"},
			sessionPolicy:     `{"Version":"2012-10-17"}`,
			requireIP:         new(true),
			dur:               new(int32(7200)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			p, _ := b.CreateProfile(context.Background(),
				"base-profile",
				[]string{"arn:aws:iam::123:role/OldRole"},
				nil,
				nil,
				nil,
				"",
				false,
			)

			updated, err := b.UpdateProfile(context.Background(),
				p.ProfileID,
				tt.newName,
				tt.roleArns,
				tt.dur,
				tt.managedPolicyArns,
				tt.sessionPolicy,
				tt.requireIP,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.newName, updated.Name)
			assert.Equal(t, tt.roleArns, updated.RoleArns)
			assert.Equal(t, tt.managedPolicyArns, updated.ManagedPolicyArns)
			assert.Equal(t, tt.sessionPolicy, updated.SessionPolicy)
			require.NotNil(t, updated.DurationSeconds)
			assert.Equal(t, int32(7200), *updated.DurationSeconds)
			assert.True(t, updated.RequireInstanceProperties)
		})
	}
}

// ---- Provider ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{"provider name is RolesAnywhere", "RolesAnywhere"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &rolesanywhere.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     *service.AppContext
		name    string
		wantErr bool
	}{
		{
			name:    "nil context returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "empty context returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
		{
			name:    "context with config provider returns handler",
			ctx:     &service.AppContext{Config: &fakeConfigProvider{}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &rolesanywhere.Provider{}
			got, err := p.Init(tt.ctx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

// fakeConfigProvider implements config.Provider for tests.
type fakeConfigProvider struct{}

func (f *fakeConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("999999999999", "us-west-2", 0, 0, false, 0)
}

// ---- helpers ----

func boolPtr(b bool) *bool { return new(b) } //nolint:unused // existing issue.

func int32Ptr(i int32) *int32 { return new(i) } //nolint:unused // existing issue.
