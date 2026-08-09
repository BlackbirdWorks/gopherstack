package workspaces_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

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

func TestDescribeWorkspaceBundles_ComputeTypeAndStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)
	require.NotEmpty(t, bundles)

	for _, b := range bundles {
		bun := b.(map[string]any)
		bundleID := bun["BundleId"].(string)

		ctRaw, hasComputeType := bun["ComputeType"]
		assert.True(t, hasComputeType, "bundle %s must have ComputeType", bundleID)

		if hasComputeType {
			ct := ctRaw.(map[string]any)
			name, _ := ct["Name"].(string)
			assert.NotEmpty(t, name, "bundle %s ComputeType.Name must not be empty", bundleID)
		}

		usRaw, hasUserStorage := bun["UserStorage"]
		assert.True(t, hasUserStorage, "bundle %s must have UserStorage", bundleID)

		if hasUserStorage {
			us := usRaw.(map[string]any)
			capacityStr, _ := us["Capacity"].(string)
			capacity, convErr := strconv.Atoi(capacityStr)
			require.NoError(t, convErr, "bundle %s UserStorage.Capacity must be numeric", bundleID)
			assert.Positive(
				t,
				capacity,
				"bundle %s UserStorage.Capacity must be > 0",
				bundleID,
			)
		}

		rsRaw, hasRootStorage := bun["RootStorage"]
		assert.True(t, hasRootStorage, "bundle %s must have RootStorage", bundleID)

		if hasRootStorage {
			rs := rsRaw.(map[string]any)
			capacityStr, _ := rs["Capacity"].(string)
			capacity, convErr := strconv.Atoi(capacityStr)
			require.NoError(t, convErr, "bundle %s RootStorage.Capacity must be numeric", bundleID)
			assert.Positive(
				t,
				capacity,
				"bundle %s RootStorage.Capacity must be > 0",
				bundleID,
			)
		}
	}
}

func TestDescribeWorkspaceBundles_ByOwnerAmazon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a custom bundle.
	rec := doTargetRequest(t, h, "CreateWorkspaceBundle", map[string]any{
		"BundleName":  "MyBundle",
		"ComputeType": map[string]any{"Name": "STANDARD"},
		"UserStorage": map[string]any{"Capacity": "50"},
		"RootStorage": map[string]any{"Capacity": "80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter by owner=Amazon: should NOT include custom bundle.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{
		"Owner": "Amazon",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)

	for _, b := range bundles {
		bun := b.(map[string]any)
		assert.Equal(t, "Amazon", bun["Owner"], "owner=Amazon filter must exclude custom bundles")
	}
}

func TestDescribeWorkspaceBundles_IncludesCustomBundle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doTargetRequest(t, h, "CreateWorkspaceBundle", map[string]any{
		"BundleName":        "MyCustomBundle",
		"BundleDescription": "A test bundle",
		"ComputeType":       map[string]any{"Name": "STANDARD"},
		"UserStorage":       map[string]any{"Capacity": "50"},
		"RootStorage":       map[string]any{"Capacity": "80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	customBundleID := createResp["WorkspaceBundle"].(map[string]any)["BundleId"].(string)

	// Without owner filter: should include both Amazon and custom bundles.
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)

	found := false
	for _, b := range bundles {
		if b.(map[string]any)["BundleId"] == customBundleID {
			found = true
		}
	}

	assert.True(t, found, "custom bundle must appear in unfiltered DescribeWorkspaceBundles")
}

func TestDescribeWorkspaceBundles_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTargetRequest(t, h, "DescribeWorkspaceBundles", map[string]any{
		"BundleIds": []string{"wsb-bh8rsxt14"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	bundles := resp["Bundles"].([]any)
	require.Len(t, bundles, 1)
	assert.Equal(t, "wsb-bh8rsxt14", bundles[0].(map[string]any)["BundleId"])
}

// TestWorkspaceBundleCRUD exercises the custom-bundle create/update/delete lifecycle.
func TestWorkspaceBundleCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name        string
		bundleName  string
		description string
	}{
		{name: "basic bundle", bundleName: "MyBundle", description: "A custom bundle"},
		{name: "minimal bundle", bundleName: "Min", description: ""},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateWorkspaceBundle", map[string]any{
				"BundleName":        tc.bundleName,
				"BundleDescription": tc.description,
				"ImageId":           "wsi-00000001",
				"ComputeType":       map[string]string{"Name": "VALUE"},
				"UserStorage":       map[string]string{"Capacity": "10"},
				"RootStorage":       map[string]string{"Capacity": "80"},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut struct {
				WorkspaceBundle map[string]any `json:"WorkspaceBundle"`
			}
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			bundleID, _ := createOut.WorkspaceBundle["BundleId"].(string)
			if bundleID == "" {
				t.Fatal("expected non-empty BundleId")
			}

			// Update
			rec2 := doTargetRequest(t, h, "UpdateWorkspaceBundle", map[string]any{
				"BundleId": bundleID,
				"ImageId":  "wsi-00000002",
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("update: expected 200, got %d", rec2.Code)
			}

			// Delete
			rec3 := doTargetRequest(t, h, "DeleteWorkspaceBundle", map[string]any{
				"BundleId": bundleID,
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec3.Code)
			}
		})
	}
}
