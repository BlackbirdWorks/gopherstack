package workspaces_test

import (
	"net/http"
	"testing"
)

func TestWorkspacesPoolCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name        string
		poolName    string
		bundleID    string
		directoryID string
	}{
		{name: "basic pool", poolName: "MyPool", bundleID: "wsb-abc", directoryID: "d-xyz"},
		{name: "second pool", poolName: "Pool2", bundleID: "wsb-def", directoryID: "d-abc"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateWorkspacesPool", map[string]any{
				"PoolName":    tc.poolName,
				"BundleId":    tc.bundleID,
				"DirectoryId": tc.directoryID,
				"Description": "test pool",
				"Capacity":    map[string]int{"DesiredUserSessions": 5},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut struct {
				WorkspacesPool map[string]any `json:"WorkspacesPool"`
			}
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			poolID, _ := createOut.WorkspacesPool["PoolId"].(string)
			if poolID == "" {
				t.Fatal("expected non-empty PoolId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeWorkspacesPools", map[string]any{
				"PoolIds": []string{poolID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			// Stop
			rec3 := doTargetRequest(t, h, "StopWorkspacesPool", map[string]any{
				"PoolId": poolID,
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("stop: expected 200, got %d", rec3.Code)
			}

			// Start
			rec4 := doTargetRequest(t, h, "StartWorkspacesPool", map[string]any{
				"PoolId": poolID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("start: expected 200, got %d", rec4.Code)
			}

			// Update
			rec5 := doTargetRequest(t, h, "UpdateWorkspacesPool", map[string]any{
				"PoolId":      poolID,
				"Description": "updated description",
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("update: expected 200, got %d", rec5.Code)
			}

			// Describe sessions
			rec6 := doTargetRequest(t, h, "DescribeWorkspacesPoolSessions", map[string]any{
				"PoolId": poolID,
			})
			if rec6.Code != http.StatusOK {
				t.Fatalf("describe sessions: expected 200, got %d", rec6.Code)
			}

			// TerminateWorkspacesPoolSession — with non-existent session (should 404)
			rec7 := doTargetRequest(t, h, "TerminateWorkspacesPoolSession", map[string]any{
				"SessionId": "no-such-session",
			})
			if rec7.Code != http.StatusNotFound {
				t.Fatalf("terminate non-existent session: expected 404, got %d", rec7.Code)
			}

			// Terminate pool
			rec8 := doTargetRequest(t, h, "TerminateWorkspacesPool", map[string]any{
				"PoolId": poolID,
			})
			if rec8.Code != http.StatusOK {
				t.Fatalf("terminate: expected 200, got %d", rec8.Code)
			}
		})
	}
}
