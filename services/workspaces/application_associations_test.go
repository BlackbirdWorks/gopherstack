package workspaces_test

import (
	"net/http"
	"testing"
)

func TestApplicationAssociations(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-test"})

	// Create a workspace first
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "alice",
				"DirectoryId": "d-test",
				"BundleId":    "wsb-test",
			},
		},
	})
	var wsOut struct {
		PendingRequests []map[string]string `json:"PendingRequests"`
	}
	decodeJSON(t, rec.Body.Bytes(), &wsOut)

	wsID := wsOut.PendingRequests[0]["WorkspaceId"]
	appID := "app-12345"

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "AssociateWorkspaceApplication",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "AssociateWorkspaceApplication", map[string]any{
					"WorkspaceId":   wsID,
					"ApplicationId": appID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeWorkspaceAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeWorkspaceAssociations", map[string]any{
					"WorkspaceId": wsID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeApplicationAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeApplicationAssociations", map[string]any{
					"ApplicationId": appID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DeployWorkspaceApplications",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DeployWorkspaceApplications", map[string]any{
					"WorkspaceId": wsID,
					"Force":       false,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeApplications",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeApplications", map[string]any{})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeImageAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeImageAssociations", map[string]any{
					"ImageId": "wsi-test",
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DescribeBundleAssociations",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DescribeBundleAssociations", map[string]any{
					"BundleId": "wsb-test",
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
		{
			name: "DisassociateWorkspaceApplication",
			fn: func(t *testing.T) {
				t.Helper()
				r := doTargetRequest(t, h, "DisassociateWorkspaceApplication", map[string]any{
					"WorkspaceId":   wsID,
					"ApplicationId": appID,
				})
				if r.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", r.Code)
				}
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t)
		})
	}
}
