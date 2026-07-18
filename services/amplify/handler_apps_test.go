package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestHandler_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "creates_app",
			body:       map[string]any{"name": "MyApp", "platform": "WEB"},
			wantStatus: http.StatusCreated,
			wantName:   "MyApp",
		},
		{
			name:       "missing_name_returns_400",
			body:       map[string]any{"platform": "WEB"},
			wantStatus: http.StatusBadRequest,
		},
		{
			// body is a JSON string (not an object) — wrong type/shape, not syntax error
			name:       "wrong_type_body_returns_400",
			body:       "not-an-object",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/apps", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				app := resp["app"].(map[string]any)
				assert.Equal(t, tt.wantName, app["name"])
			}
		})
	}
}

func TestHandler_CreateApp_MalformedJSON(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	rec := doRawRequest(t, h, http.MethodPost, "/apps", []byte(malformedJSON))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "returns_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend)
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns_empty_list",
			setup:      func(_ *amplify.InMemoryBackend) {},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "returns_all_apps",
			setup: func(b *amplify.InMemoryBackend) {
				_, _ = b.CreateApp("App1", "", "", "", nil)
				_, _ = b.CreateApp("App2", "", "", "", nil)
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps", nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			apps := resp["apps"].([]any)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestHandler_ListAppsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryString   string
		wantCount     int
		wantNextToken bool
	}{
		{
			name:        "no_pagination_returns_all",
			queryString: "",
			wantCount:   4,
		},
		{
			name:          "first_page",
			queryString:   "?maxResults=2",
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:        "second_page",
			queryString: "?maxResults=2&nextToken=2",
			wantCount:   2,
		},
		{
			name:        "token_beyond_end",
			queryString: "?maxResults=2&nextToken=100",
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()

			for _, name := range []string{"App1", "App2", "App3", "App4"} {
				_, err := b.CreateApp(name, "", "", "", nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/apps"+tt.queryString, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			apps := resp["apps"].([]any)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextToken"])
			} else {
				assert.Empty(t, resp["nextToken"])
			}
		})
	}
}

func TestHandler_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "deletes_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodDelete, "/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_GetAndDeleteApp_NotFound verifies Get/Delete on a nonexistent
// app both return 404, exercised directly through the top-level Handler().
func TestHandler_GetAndDeleteApp_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/amplify/v1/apps/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_UpdateApp verifies POST /apps/{appId} returns the updated app.
func TestHandler_UpdateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "updates_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				return seedApp(t, b, "OldName").AppID
			},
			body:       map[string]any{"name": "NewName"},
			wantStatus: http.StatusOK,
			wantName:   "NewName",
		},
		{
			name: "returns_404_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"name": "X"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				app := resp["app"].(map[string]any)
				assert.Equal(t, tt.wantName, app["name"])
			}
		})
	}
}
