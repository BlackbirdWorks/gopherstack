package serverlessrepo_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*serverlessrepo.Handler)
		name     string
		wantLen  int
		wantCode int
	}{
		{
			name:     "empty list",
			wantLen:  0,
			wantCode: http.StatusOK,
		},
		{
			name: "list with applications",
			setup: func(h *serverlessrepo.Handler) {
				_, _ = h.Backend.CreateApplication("app-a", "A", "author", "", "1.0.0", nil, "", "", "")
				_, _ = h.Backend.CreateApplication("app-b", "B", "author", "", "1.0.0", nil, "", "", "")
			},
			wantLen:  2,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			apps, ok := resp["applications"].([]any)
			require.True(t, ok)
			assert.Len(t, apps, tt.wantLen)
		})
	}
}

func TestListApplications_IncludesLabels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "", []string{"label1"}, "", "", "")
	require.NoError(t, err)

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok := resp["applications"].([]any)
	require.True(t, ok)
	require.Len(t, apps, 1)

	app := apps[0].(map[string]any)
	labels, ok := app["labels"].([]any)
	require.True(t, ok)
	assert.Len(t, labels, 1)
}

func TestListApplications_PageBoundaryExact(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-1", "app-2", "app-3", "app-4"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	// Page 1: 2 items
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r1))
	apps1 := r1["applications"].([]any)
	assert.Len(t, apps1, 2)
	nt := r1["nextToken"].(string)

	// Page 2: 2 items
	rec = doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2&nextToken="+nt, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var r2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r2))
	apps2 := r2["applications"].([]any)
	assert.Len(t, apps2, 2)
	assert.Nil(t, r2["nextToken"], "no more pages when exactly on boundary")
}

func TestListApplications_Pagination_MaxItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-a", "app-b", "app-c"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	apps, ok := resp["applications"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 2, "maxItems=2 should return 2 items")
	assert.NotNil(t, resp["nextToken"], "nextToken must be set when more items remain")
}

func TestListApplications_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"app-a", "app-b", "app-c"} {
		_, err := h.Backend.CreateApplication(name, "desc", "author", "", "", nil, "", "", "")
		require.NoError(t, err)
	}

	// First page: maxItems=2
	rec1 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	nextToken, ok := resp1["nextToken"].(string)
	require.True(t, ok, "nextToken should be a string")

	// Second page
	rec2 := doServerlessRepoRequest(t, h, http.MethodGet, "/applications?maxItems=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	apps, ok := resp2["applications"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 1, "second page should have 1 remaining item")
	assert.Nil(t, resp2["nextToken"], "no more pages - nextToken should be absent")
}
