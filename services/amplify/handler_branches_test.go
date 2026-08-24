package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestHandler_CreateBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "creates_branch",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       map[string]any{"branchName": "main", "stage": "PRODUCTION", "enableAutoBuild": true},
			wantStatus: http.StatusCreated,
		},
		{
			name: "missing_branch_name_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       map[string]any{"stage": "PRODUCTION"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_branch_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
			body:       map[string]any{"branchName": "main"},
			wantStatus: http.StatusBadRequest,
		},
		{
			// body is a JSON string (not an object) — wrong type/shape, not syntax error
			name: "wrong_type_body_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			body:       "not-an-object",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID+"/branches", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateBranch_MalformedJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	app, _ := b.CreateApp("TestApp", "", "", "", nil)
	rec := doRawRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/branches", []byte(malformedJSON))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "returns_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID+"/branches/"+branchName, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "returns_branches",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
			wantStatus: http.StatusOK,
		},
		{
			// ListBranches' own deserializeOpError switch
			// (aws-sdk-go-v2/service/amplify@v1.41.4 deserializers.go) does not
			// type NotFoundException at all -- only BadRequestException,
			// InternalFailureException, UnauthorizedException -- so an
			// unrecognized appId is reported as invalid input, not 404.
			name: "returns_400_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID := tt.setup(b)
			rec := doRequest(t, h, http.MethodGet, "/apps/"+appID+"/branches", nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListBranchesPagination(t *testing.T) {
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
			app, err := b.CreateApp("PaginationApp", "", "", "", nil)
			require.NoError(t, err)

			for _, name := range []string{"br1", "br2", "br3", "br4"} {
				_, err = b.CreateBranch(app.AppID, name, "", "", false, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/branches"+tt.queryString, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			branches := resp["branches"].([]any)
			assert.Len(t, branches, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextToken"])
			} else {
				assert.Empty(t, resp["nextToken"])
			}
		})
	}
}

func TestHandler_DeleteBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "deletes_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodDelete, "/apps/"+appID+"/branches/"+branchName, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				branch, ok := resp["branch"].(map[string]any)
				require.True(
					t, ok, "expected a \"branch\" key wrapping the deleted Branch, got body %s", rec.Body.String(),
				)
				assert.Equal(t, branchName, branch["branchName"])
			}
		})
	}
}

// TestHandler_Branch_NotFound verifies Get/List branch on a nonexistent app
// both return 404, exercised directly through the top-level Handler().
func TestHandler_Branch_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent/branches/main", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent/branches", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_UpdateBranch verifies POST /apps/{appId}/branches/{branchName} returns updated branch.
func TestHandler_UpdateBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) (string, string)
		body       any
		name       string
		wantDesc   string
		wantStatus int
	}{
		{
			name: "updates_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app := seedApp(t, b, "App1")
				seedMainBranch(t, b, app.AppID)

				return app.AppID, "main"
			},
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			wantDesc:   "updated",
		},
		{
			name: "returns_404_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				return seedApp(t, b, "App2").AppID, "nonexistent"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			appID, branchName := tt.setup(b)
			rec := doRequest(t, h, http.MethodPost, "/apps/"+appID+"/branches/"+branchName, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDesc != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				br := resp["branch"].(map[string]any)
				assert.Equal(t, tt.wantDesc, br["description"])
			}
		})
	}
}
