package workspaces_test

import (
	"net/http"
	"testing"
)

// TestWorkspacesPool_CapacityStatusAndRunningMode verifies fields that were
// previously entirely missing from the WorkspacesPool wire shape:
// CapacityStatus and RunningMode are both `This member is required` on the
// real type, and CreatedAt must be a wire-format epoch-seconds number, not an
// ISO8601 string.
func TestWorkspacesPool_CapacityStatusAndRunningMode(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	rec := doTargetRequest(t, h, "CreateWorkspacesPool", map[string]any{
		"PoolName":    "cap-test-pool",
		"BundleId":    "wsb-abc",
		"DirectoryId": "d-xyz",
		"Description": "test pool",
		"RunningMode": "AUTO_STOP",
		"Capacity":    map[string]int{"DesiredUserSessions": 10},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
	}

	var out struct {
		WorkspacesPool struct {
			RunningMode    string  `json:"RunningMode"`
			CreatedAt      float64 `json:"CreatedAt"`
			CapacityStatus struct {
				ActiveUserSessions    int32 `json:"ActiveUserSessions"`
				ActualUserSessions    int32 `json:"ActualUserSessions"`
				AvailableUserSessions int32 `json:"AvailableUserSessions"`
				DesiredUserSessions   int32 `json:"DesiredUserSessions"`
			} `json:"CapacityStatus"`
		} `json:"WorkspacesPool"`
	}
	decodeJSON(t, rec.Body.Bytes(), &out)

	pool := out.WorkspacesPool

	if pool.RunningMode != "AUTO_STOP" {
		t.Fatalf("expected RunningMode=AUTO_STOP, got %s", pool.RunningMode)
	}

	if pool.CreatedAt <= 0 {
		t.Fatalf("expected positive numeric CreatedAt, got %v", pool.CreatedAt)
	}

	if pool.CapacityStatus.DesiredUserSessions != 10 {
		t.Fatalf("expected DesiredUserSessions=10, got %d", pool.CapacityStatus.DesiredUserSessions)
	}

	if pool.CapacityStatus.ActiveUserSessions != 0 {
		t.Fatalf("expected ActiveUserSessions=0, got %d", pool.CapacityStatus.ActiveUserSessions)
	}

	if pool.CapacityStatus.ActualUserSessions != pool.CapacityStatus.AvailableUserSessions+
		pool.CapacityStatus.ActiveUserSessions {
		t.Fatalf(
			"CapacityStatus invariant broken: Actual=%d Available=%d Active=%d",
			pool.CapacityStatus.ActualUserSessions,
			pool.CapacityStatus.AvailableUserSessions,
			pool.CapacityStatus.ActiveUserSessions,
		)
	}
}

// TestWorkspacesPool_UpdateRunningModeAndCapacity verifies UpdateWorkspacesPool
// applies RunningMode and Capacity, previously silently dropped fields.
func TestWorkspacesPool_UpdateRunningModeAndCapacity(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	rec := doTargetRequest(t, h, "CreateWorkspacesPool", map[string]any{
		"PoolName":    "update-test-pool",
		"BundleId":    "wsb-abc",
		"DirectoryId": "d-xyz",
		"Description": "test pool",
		"Capacity":    map[string]int{"DesiredUserSessions": 3},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
	}

	var createOut struct {
		WorkspacesPool struct {
			PoolId string `json:"PoolId"` //nolint:revive,staticcheck // AWS wire casing.
		} `json:"WorkspacesPool"`
	}
	decodeJSON(t, rec.Body.Bytes(), &createOut)

	rec2 := doTargetRequest(t, h, "UpdateWorkspacesPool", map[string]any{
		"PoolId":      createOut.WorkspacesPool.PoolId,
		"RunningMode": "ALWAYS_ON",
		"Capacity":    map[string]int{"DesiredUserSessions": 20},
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec2.Code, rec2.Body)
	}

	var updateOut struct {
		WorkspacesPool struct {
			RunningMode    string `json:"RunningMode"`
			CapacityStatus struct {
				DesiredUserSessions int32 `json:"DesiredUserSessions"`
			} `json:"CapacityStatus"`
		} `json:"WorkspacesPool"`
	}
	decodeJSON(t, rec2.Body.Bytes(), &updateOut)

	if updateOut.WorkspacesPool.RunningMode != "ALWAYS_ON" {
		t.Fatalf("expected RunningMode=ALWAYS_ON, got %s", updateOut.WorkspacesPool.RunningMode)
	}

	if updateOut.WorkspacesPool.CapacityStatus.DesiredUserSessions != 20 {
		t.Fatalf(
			"expected DesiredUserSessions=20, got %d",
			updateOut.WorkspacesPool.CapacityStatus.DesiredUserSessions,
		)
	}
}

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
