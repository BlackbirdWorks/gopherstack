package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestHandler_ListApplicationDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "returns empty dependencies list",
			appName:  "my-app",
			wantCode: http.StatusOK,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName+"/dependencies", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				deps, ok := resp["dependencies"].([]any)
				require.True(t, ok)
				assert.Empty(t, deps)
			}
		})
	}
}

func TestListApplicationDependencies_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/dep-app/dependencies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	deps, ok := resp["dependencies"].([]any)
	require.True(t, ok, "dependencies must be an array (not nil)")
	assert.Empty(t, deps)
}

func TestListApplicationDependencies_NoSemanticVersion_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-no-sv-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationDependencyInternal("dep-no-sv-app", "1.0.0",
		serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/child",
			SemanticVersion: "1.0.0",
		}))

	h := serverlessrepo.NewHandler(b)

	// Query for a different version — should return empty
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-no-sv-app/dependencies?semanticVersion=2.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	deps := resp["dependencies"].([]any)
	assert.Empty(t, deps)
}

func TestListApplicationDependencies_PersistedState(t *testing.T) {
	t.Parallel()

	tests := []serverlessrepo.ApplicationDependency{
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-a",
			SemanticVersion: "1.0.0",
		},
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-b",
			SemanticVersion: "2.1.0",
		},
		{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-c",
			SemanticVersion: "4.0.0",
		},
	}

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("root-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)
	for _, dependency := range tests[:2] {
		require.NoError(t, b.AddApplicationDependencyInternal("root-app", "3.0.0", dependency))
	}
	_, err = b.CreateApplication("nested-a", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)
	require.NoError(t, b.AddApplicationDependencyInternal("nested-a", "1.0.0", tests[2]))

	restored := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), b.Snapshot(t.Context())))
	h := serverlessrepo.NewHandler(restored)
	rec := doServerlessRepoRequest(
		t,
		h,
		http.MethodGet,
		"/applications/root-app/dependencies?semanticVersion=3.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var response struct {
		Dependencies []serverlessrepo.ApplicationDependency `json:"dependencies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.ElementsMatch(t, tests, response.Dependencies)
}

func TestListApplicationDependencies_Pagination(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	deps := []serverlessrepo.ApplicationDependency{
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-a", SemanticVersion: "1.0.0"},
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-b", SemanticVersion: "1.0.0"},
		{ApplicationID: "arn:aws:serverlessrepo:us-east-1:000000000000:applications/app-c", SemanticVersion: "1.0.0"},
	}

	for _, dep := range deps {
		require.NoError(t, b.AddApplicationDependencyInternal("dep-app", "1.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)

	// First page: maxItems=2
	rec := doServerlessRepoRequest(
		t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))
	page1, ok := resp1["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	nextToken, ok := resp1["nextToken"].(string)
	require.True(t, ok, "nextToken must be present when more items remain")
	assert.NotEmpty(t, nextToken)

	// Second page
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=1.0.0&maxItems=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	page2, ok := resp2["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)
	assert.Nil(t, resp2["nextToken"], "no more pages")
}

func TestListApplicationDependencies_MaxItemsDefault(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("dep-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	for i := range 3 {
		dep := serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/nested-" + string(rune('a'+i)),
			SemanticVersion: "1.0.0",
		}
		require.NoError(t, b.AddApplicationDependencyInternal("dep-app", "2.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/dep-app/dependencies?semanticVersion=2.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	page, ok := resp["dependencies"].([]any)
	require.True(t, ok)
	assert.Len(t, page, 3)
	assert.Nil(t, resp["nextToken"], "all 3 fit within default maxItems=100")
}

func TestListApplicationDependencies_DeterministicOrder(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("order-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	// Insert in reverse alphabetical order.
	for _, name := range []string{"zzz-app", "aaa-app", "mmm-app"} {
		dep := serverlessrepo.ApplicationDependency{
			ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/" + name,
			SemanticVersion: "1.0.0",
		}
		require.NoError(t, b.AddApplicationDependencyInternal("order-app", "1.0.0", dep))
	}

	h := serverlessrepo.NewHandler(b)
	rec := doServerlessRepoRequest(t, h, http.MethodGet,
		"/applications/order-app/dependencies?semanticVersion=1.0.0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	page, ok := resp["dependencies"].([]any)
	require.True(t, ok)
	require.Len(t, page, 3)

	ids := make([]string, 3)
	for i, d := range page {
		ids[i] = d.(map[string]any)["applicationId"].(string)
	}

	assert.True(t, ids[0] < ids[1] && ids[1] < ids[2], "dependencies must be sorted alphabetically by applicationId")
}
