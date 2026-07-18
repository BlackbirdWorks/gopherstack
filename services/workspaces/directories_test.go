package workspaces_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

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

func TestDescribeWorkspaceDirectories_AfterRegister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Before registration: no directories returned.
	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	assert.Empty(t, dirs, "DescribeWorkspaceDirectories must be empty before registration")

	// Register a directory.
	rec2 := doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-abc123456",
		"SubnetIds":   []string{"subnet-aaa", "subnet-bbb"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// After registration: directory must appear.
	rec3 := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	dirs3 := resp3["Directories"].([]any)
	require.Len(t, dirs3, 1)

	dir := dirs3[0].(map[string]any)
	assert.Equal(t, "d-abc123456", dir["DirectoryId"])
	assert.Equal(t, "REGISTERED", dir["State"])

	subnetIDs, ok := dir["SubnetIds"].([]any)
	require.True(t, ok, "SubnetIds must be present after registration with subnets")
	assert.Len(t, subnetIDs, 2)
}

func TestDescribeWorkspaceDirectories_FilterByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-aaaa",
	})
	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-bbbb",
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{
		"DirectoryIds": []string{"d-aaaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	require.Len(t, dirs, 1)
	assert.Equal(t, "d-aaaa", dirs[0].(map[string]any)["DirectoryId"])
}

func TestDescribeWorkspaceDirectories_AfterDeregister(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-xyz",
	})

	doTargetRequest(t, h, "DeregisterWorkspaceDirectory", map[string]any{
		"DirectoryId": "d-xyz",
	})

	rec := doTargetRequest(t, h, "DescribeWorkspaceDirectories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dirs := resp["Directories"].([]any)
	assert.Empty(t, dirs, "deregistered directory must not appear")
}

// TestDirectoryRegistration exercises the register/deregister lifecycle via the handler.
func TestDirectoryRegistration(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name        string
		directoryID string
	}{
		{name: "register simple", directoryID: "d-0123456789"},
		{name: "register another", directoryID: "d-abcdef1234"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Register
			rec := doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
				"DirectoryId": tc.directoryID,
				"SubnetIds":   []string{"subnet-1", "subnet-2"},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("register: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var regOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &regOut)

			if regOut["DirectoryId"] != tc.directoryID {
				t.Fatalf("expected DirectoryId=%s, got %s", tc.directoryID, regOut["DirectoryId"])
			}

			// Deregister
			rec2 := doTargetRequest(t, h, "DeregisterWorkspaceDirectory", map[string]any{
				"DirectoryId": tc.directoryID,
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("deregister: expected 200, got %d", rec2.Code)
			}
		})
	}
}
