package appconfig_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_Application_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantName   string
		body       []byte
		wantStatus int
	}{
		{
			name:       "create application",
			method:     http.MethodPost,
			path:       "/applications",
			body:       []byte(`{"name":"my-app","description":"test"}`),
			wantStatus: http.StatusCreated,
			wantName:   "my-app",
		},
		{
			name:       "list applications empty",
			method:     http.MethodGet,
			path:       "/applications",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var app appconfig.Application
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))
				assert.Equal(t, tt.wantName, app.Name)
			}
		})
	}
}

func TestHandler_GetApplication_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/applications/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an application first.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"delete-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Delete it.
	rec = doRequest(t, h, http.MethodDelete, "/applications/"+app.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Get should return 404 now.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"original"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/applications/"+app.ID,
		[]byte(`{"name":"updated","description":"new desc"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "updated", updated.Name)
	assert.Equal(t, "new desc", updated.Description)
}

// TestHandler_UpdateApplication_OmittedDescriptionPreserved verifies that
// omitting Description from an UpdateApplication request leaves the
// existing description untouched, matching real AWS AppConfig's
// UpdateApplicationInput.Description (an optional *string member: absent
// means unchanged, only a present value -- including "" -- overwrites).
func TestHandler_UpdateApplication_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications",
		[]byte(`{"Name":"orig","Description":"keep-me"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Update only Name; Description is omitted from the request body.
	rec = doRequest(t, h, http.MethodPatch, "/applications/"+app.ID,
		[]byte(`{"Name":"renamed"}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "renamed", updated.Name)
	assert.Equal(t, "keep-me", updated.Description,
		"omitted Description must not clobber the existing value")
}

func TestHandler_DeleteApplication_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/applications/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApplication_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPatch, "/applications/nonexistent", []byte(`{"name":"new"}`))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateApplication_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"description":"no name"}`))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListApplicationsPagination(t *testing.T) {
	t.Parallel()

	t.Run("no_pagination_returns_all", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		rec := doRequest(t, h, http.MethodGet, "/applications", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 4)
		assert.Empty(t, resp.NextToken)
	})

	t.Run("first_page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		rec := doRequest(t, h, http.MethodGet, "/applications?max_results=2", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 2)
		assert.NotEmpty(t, resp.NextToken)
	})

	t.Run("second_page", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		// Get the first page to obtain a valid HMAC token.
		rec := doRequest(t, h, http.MethodGet, "/applications?max_results=2", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		var first struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))
		require.NotEmpty(t, first.NextToken)

		rec2 := doRequest(
			t,
			h,
			http.MethodGet,
			"/applications?max_results=2&next_token="+first.NextToken,
			nil,
		)
		require.Equal(t, http.StatusOK, rec2.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
		assert.Len(t, resp.Items, 2)
		assert.Empty(t, resp.NextToken)
	})

	t.Run("token_beyond_end", func(t *testing.T) {
		t.Parallel()
		backend := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
		h2 := appconfig.NewHandler(backend)
		for _, name := range []string{"app1", "app2", "app3", "app4"} {
			rec := doRequest(t, h2, http.MethodPost, "/applications", []byte(`{"name":"`+name+`"}`))
			require.Equal(t, http.StatusCreated, rec.Code)
		}
		beyondToken := page.EncodeHMACToken(100, backend.PaginationSecret())
		rec := doRequest(
			t,
			h2,
			http.MethodGet,
			"/applications?max_results=2&next_token="+beyondToken,
			nil,
		)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			NextToken string `json:"NextToken"`
			Items     []any  `json:"Items"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Empty(t, resp.Items)
		assert.Empty(t, resp.NextToken)
	})
}

// TestHandler_CreateApplication_TagsAppliedInline verifies that Tags sent
// inline on CreateApplicationInput are visible via ListTagsForResource
// immediately after creation -- previously CreateApplication's handler never
// bound or forwarded the Tags field at all, so tags set at create time
// silently vanished (bd gopherstack-lcan).
func TestHandler_CreateApplication_TagsAppliedInline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "tags_applied_at_create",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "no_tags_is_not_an_error",
			tags: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body, err := json.Marshal(map[string]any{
				"Name": "tagged-app-" + tt.name,
				"Tags": tt.tags,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/applications", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var app appconfig.Application
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

			resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID
			tagsRec := doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
			require.Equal(t, http.StatusOK, tagsRec.Code)

			var tagsResp struct {
				Tags map[string]string `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))

			if len(tt.tags) == 0 {
				assert.Empty(t, tagsResp.Tags)
			} else {
				assert.Equal(t, tt.tags, tagsResp.Tags)
			}
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/applications/app-abc", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "app-abc", h.ExtractResource(c))
}
