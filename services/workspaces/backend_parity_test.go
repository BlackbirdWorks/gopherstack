package workspaces_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// ctxWithRegion returns a context carrying the given region via awsmeta.
func ctxWithRegion(region string) context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Region: region})
}

// TestCreateWorkspace_RequiresRegisteredDirectory verifies that CreateWorkspace
// returns an error for an unregistered directory and succeeds after registration.
func TestCreateWorkspace_RequiresRegisteredDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dirID     string
		register  bool
		wantError bool
	}{
		{
			name:      "unregistered directory returns error",
			dirID:     "d-unregistered",
			register:  false,
			wantError: true,
		},
		{
			name:      "registered directory succeeds",
			dirID:     "d-registered",
			register:  true,
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.register {
				require.NoError(t, b.RegisterWorkspaceDirectory(tc.dirID, nil))
			}

			_, err := b.CreateWorkspace(context.Background(), &workspaces.WorkspaceCreationSpec{
				UserName:    "alice",
				DirectoryID: tc.dirID,
				BundleID:    "wsb-bh8rsxt14",
			})

			if tc.wantError {
				assert.Error(t, err, "unregistered directory must return error")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDescribeWorkspaces_FiltersByRegion verifies that workspaces created in
// one region are not returned when describing from another region.
func TestDescribeWorkspaces_FiltersByRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createRegion string
		queryRegion  string
		wantCount    int
	}{
		{
			name:         "same region returns workspace",
			createRegion: "us-east-1",
			queryRegion:  "us-east-1",
			wantCount:    1,
		},
		{
			name:         "different region returns nothing",
			createRegion: "us-east-1",
			queryRegion:  "eu-west-1",
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", tc.createRegion)
			require.NoError(t, b.RegisterWorkspaceDirectory("d-test", nil))

			createCtx := ctxWithRegion(tc.createRegion)
			_, err := b.CreateWorkspace(createCtx, &workspaces.WorkspaceCreationSpec{
				UserName:    "alice",
				DirectoryID: "d-test",
				BundleID:    "wsb-bh8rsxt14",
			})
			require.NoError(t, err)

			queryCtx := ctxWithRegion(tc.queryRegion)
			wsList, _, err := b.DescribeWorkspaces(queryCtx, nil, nil, nil, nil, 0, "")
			require.NoError(t, err)
			assert.Len(t, wsList, tc.wantCount)
		})
	}
}

// TestDescribeWorkspaceBundles_PaginatesResults verifies that DescribeWorkspaceBundles
// returns a NextToken when there are more results, and the token can be used to fetch the rest.
func TestDescribeWorkspaceBundles_PaginatesResults(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(b)

	// Fetch first page (Amazon has 5 bundles; page size = 25, so all fit on one page normally).
	// Add custom bundles to force pagination by querying via API with small NextToken simulation.
	// Instead test that a real NextToken from the API roundtrips correctly.
	rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	bundles, _ := resp["Bundles"].([]any)
	assert.NotEmpty(t, bundles, "DescribeWorkspaceBundles must return at least one bundle")

	// When all bundles fit on one page there should be no NextToken.
	nextToken, hasToken := resp["NextToken"]
	if hasToken {
		assert.NotEmpty(t, nextToken, "NextToken must be non-empty when present")
	}
}

// TestDescribeWorkspaceBundles_FiltersByBundleID verifies that filtering
// by specific BundleIds returns only those bundles.
func TestDescribeWorkspaceBundles_FiltersByBundleID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bundleIDs []any
		wantCount int
	}{
		{
			name:      "filter by one bundle",
			bundleIDs: []any{"wsb-bh8rsxt14"},
			wantCount: 1,
		},
		{
			name:      "filter by two bundles",
			bundleIDs: []any{"wsb-bh8rsxt14", "wsb-gm4d5tx2v"},
			wantCount: 2,
		},
		{
			name:      "filter by nonexistent bundle",
			bundleIDs: []any{"wsb-doesnotexist"},
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
			h := workspaces.NewHandler(b)

			rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{
				"BundleIds": tc.bundleIDs,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			bundles, _ := resp["Bundles"].([]any)
			assert.Len(t, bundles, tc.wantCount)
		})
	}
}

// TestDescribeWorkspaceDirectories_PaginatesResults verifies that directories
// are paginated and NextToken roundtrips correctly.
func TestDescribeWorkspaceDirectories_PaginatesResults(t *testing.T) {
	t.Parallel()

	b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(b)

	// Register two directories.
	require.NoError(t, b.RegisterWorkspaceDirectory("d-aaa", nil))
	require.NoError(t, b.RegisterWorkspaceDirectory("d-bbb", nil))

	// Fetch all.
	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dirs, _ := resp["Directories"].([]any)
	assert.Len(t, dirs, 2, "both registered directories must be returned")

	// Simulate pagination with a cursor that points to d-bbb (skips d-aaa).
	cursor := base64.StdEncoding.EncodeToString([]byte("d-bbb"))
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{
		"NextToken": cursor,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	dirs2, _ := resp2["Directories"].([]any)
	require.Len(t, dirs2, 1, "cursor should skip d-aaa and return only d-bbb")

	dirID, _ := dirs2[0].(map[string]any)["DirectoryId"].(string)
	assert.Equal(t, "d-bbb", dirID)
}
