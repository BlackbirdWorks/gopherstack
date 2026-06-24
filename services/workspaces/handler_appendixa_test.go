package workspaces_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// newTestHandlerWithBackend creates a handler and returns both handler and backend.
func newTestHandlerWithBackend(
	t *testing.T,
) (*workspaces.Handler, *workspaces.InMemoryBackend) { //nolint:unparam // existing issue.
	t.Helper()

	b := workspaces.NewInMemoryBackend("111122223333", "us-east-1")
	h := workspaces.NewHandler(b)

	return h, b
}

func decodeJSON(t *testing.T, body []byte, dst any) {
	t.Helper()

	if err := json.Unmarshal(body, dst); err != nil {
		t.Fatalf("unmarshal response: %v (body=%s)", err, body)
	}
}

// dispatchCheck dispatches a JSON request and checks the status code.
func dispatchCheck( //nolint:unused // existing issue.
	t *testing.T,
	h *workspaces.Handler,
	target string,
	body any,
	wantCode int,
) *httptest.ResponseRecorder {
	t.Helper()

	rec := doTargetRequest(t, h, target, body)
	if rec.Code != wantCode {
		t.Fatalf("%s: expected %d, got %d: %s", target, wantCode, rec.Code, rec.Body)
	}

	return rec
}

// =============================================================================
// IP Group tests
// ============================================================================= //nolint:godot // existing issue.
func TestIpGroupCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name      string
		groupName string
		rules     []map[string]string
	}{
		{
			name:      "simple group",
			groupName: "test-group",
			rules:     []map[string]string{{"ipRule": "10.0.0.0/8", "ruleDesc": "internal"}},
		},
		{
			name:      "empty rules group",
			groupName: "empty-group",
			rules:     nil,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateIpGroup", map[string]any{
				"GroupName": tc.groupName,
				"GroupDesc": "desc",
				"UserRules": tc.rules,
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			groupID := createOut["GroupId"]
			if groupID == "" {
				t.Fatal("expected non-empty GroupId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeIpGroups", map[string]any{
				"GroupIds": []string{groupID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				Result []map[string]any `json:"Result"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.Result) != 1 {
				t.Fatalf("expected 1 group, got %d", len(descOut.Result))
			}

			// Authorize rules
			rec3 := doTargetRequest(t, h, "AuthorizeIpRules", map[string]any{
				"GroupId":   groupID,
				"UserRules": []map[string]string{{"ipRule": "192.168.0.0/16", "ruleDesc": "extra"}},
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("authorize: expected 200, got %d", rec3.Code)
			}

			// Update rules
			rec4 := doTargetRequest(t, h, "UpdateRulesOfIpGroup", map[string]any{
				"GroupId":   groupID,
				"UserRules": []map[string]string{{"ipRule": "172.16.0.0/12", "ruleDesc": "new"}},
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("update rules: expected 200, got %d", rec4.Code)
			}

			// Revoke rules
			rec5 := doTargetRequest(t, h, "RevokeIpRules", map[string]any{
				"GroupId":   groupID,
				"UserRules": []string{"172.16.0.0/12"},
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("revoke: expected 200, got %d", rec5.Code)
			}

			// Associate with directory
			rec6 := doTargetRequest(t, h, "AssociateIpGroups", map[string]any{
				"DirectoryId": "d-123",
				"GroupIds":    []string{groupID},
			})
			if rec6.Code != http.StatusOK {
				t.Fatalf("associate: expected 200, got %d", rec6.Code)
			}

			// Disassociate
			rec7 := doTargetRequest(t, h, "DisassociateIpGroups", map[string]any{
				"DirectoryId": "d-123",
				"GroupIds":    []string{groupID},
			})
			if rec7.Code != http.StatusOK {
				t.Fatalf("disassociate: expected 200, got %d", rec7.Code)
			}

			// Delete
			rec8 := doTargetRequest(t, h, "DeleteIpGroup", map[string]any{
				"GroupId": groupID,
			})
			if rec8.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec8.Code)
			}

			// Describe after delete — should be empty
			rec9 := doTargetRequest(t, h, "DescribeIpGroups", map[string]any{
				"GroupIds": []string{groupID},
			})
			var afterDelete struct {
				Result []any `json:"Result"`
			}
			decodeJSON(t, rec9.Body.Bytes(), &afterDelete)

			if len(afterDelete.Result) != 0 {
				t.Fatalf("expected 0 groups after delete, got %d", len(afterDelete.Result))
			}
		})
	}
}

// =============================================================================
// Connection Alias tests
// ============================================================================= //nolint:godot // existing issue.
func TestConnectionAliasCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name             string
		connectionString string
	}{
		{name: "simple alias", connectionString: "myalias.corp.example"},
		{name: "ip alias", connectionString: "10.0.0.1"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateConnectionAlias", map[string]any{
				"ConnectionString": tc.connectionString,
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			aliasID := createOut["AliasId"]
			if aliasID == "" {
				t.Fatal("expected non-empty AliasId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeConnectionAliases", map[string]any{
				"AliasIds": []string{aliasID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				ConnectionAliases []map[string]any `json:"ConnectionAliases"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.ConnectionAliases) != 1 {
				t.Fatalf("expected 1 alias, got %d", len(descOut.ConnectionAliases))
			}

			// Associate
			rec3 := doTargetRequest(t, h, "AssociateConnectionAlias", map[string]any{
				"AliasId":    aliasID,
				"ResourceId": "res-123",
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("associate: expected 200, got %d", rec3.Code)
			}

			// Describe permissions
			rec4 := doTargetRequest(t, h, "DescribeConnectionAliasPermissions", map[string]any{
				"AliasId": aliasID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("describe perms: expected 200, got %d", rec4.Code)
			}

			// Update permission
			rec5 := doTargetRequest(t, h, "UpdateConnectionAliasPermission", map[string]any{
				"AliasId": aliasID,
				"ConnectionAliasPermission": map[string]any{
					"SharedAccountId":  "999988887777",
					"AllowAssociation": true,
				},
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("update perm: expected 200, got %d", rec5.Code)
			}

			// Disassociate
			rec6 := doTargetRequest(t, h, "DisassociateConnectionAlias", map[string]any{
				"AliasId": aliasID,
			})
			if rec6.Code != http.StatusOK {
				t.Fatalf("disassociate: expected 200, got %d", rec6.Code)
			}

			// Delete
			rec7 := doTargetRequest(t, h, "DeleteConnectionAlias", map[string]any{
				"AliasId": aliasID,
			})
			if rec7.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec7.Code)
			}
		})
	}
}

// =============================================================================
// Bundle tests
// ============================================================================= //nolint:godot // existing issue.
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
				"UserStorage":       map[string]int32{"Capacity": 10},
				"RootStorage":       map[string]int32{"Capacity": 80},
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

// =============================================================================
// Image tests
// ============================================================================= //nolint:godot // existing issue.
func TestWorkspaceImageCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	tests := []struct {
		body  any
		check func(t *testing.T, body []byte)
		name  string
		op    string
	}{
		{
			name: "CopyWorkspaceImage",
			op:   "CopyWorkspaceImage",
			body: map[string]any{
				"Name":          "copied-image",
				"SourceImageId": "wsi-source",
				"SourceRegion":  "us-west-2",
				"Description":   "A copied image",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
		{
			name: "CreateWorkspaceImage",
			op:   "CreateWorkspaceImage",
			body: map[string]any{
				"Name":        "new-image",
				"Description": "from workspace",
				"WorkspaceId": "ws-00000001",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
				if out["State"] != "AVAILABLE" {
					t.Fatalf("expected AVAILABLE state, got %s", out["State"])
				}
			},
		},
		{
			name: "ImportWorkspaceImage",
			op:   "ImportWorkspaceImage",
			body: map[string]any{
				"Ec2ImageId":       "ami-12345678",
				"ImageName":        "imported",
				"ImageDescription": "ec2 import",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
		{
			name: "ImportCustomWorkspaceImage",
			op:   "ImportCustomWorkspaceImage",
			body: map[string]any{
				"ImageName":        "custom-img",
				"ImageDescription": "custom",
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				decodeJSON(t, body, &out)
				if out["ImageId"] == "" {
					t.Fatal("expected ImageId")
				}
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
			}

			tc.check(t, rec.Body.Bytes())
		})
	}
}

func TestWorkspaceImageDescribeAndPermissions(
	t *testing.T,
) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	// Create an image
	rec := doTargetRequest(t, h, "CopyWorkspaceImage", map[string]any{
		"Name":          "perm-test",
		"SourceImageId": "wsi-src",
		"SourceRegion":  "us-east-1",
	})
	var createOut map[string]string
	decodeJSON(t, rec.Body.Bytes(), &createOut)
	imageID := createOut["ImageId"]

	// Describe images
	rec2 := doTargetRequest(t, h, "DescribeWorkspaceImages", map[string]any{
		"ImageIds": []string{imageID},
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe images: expected 200, got %d", rec2.Code)
	}

	var descOut struct {
		Images []map[string]any `json:"Images"`
	}
	decodeJSON(t, rec2.Body.Bytes(), &descOut)

	if len(descOut.Images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(descOut.Images))
	}

	// Update permission
	rec3 := doTargetRequest(t, h, "UpdateWorkspaceImagePermission", map[string]any{
		"ImageId":         imageID,
		"SharedAccountId": "999988887777",
		"AllowCopyImage":  true,
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("update permission: expected 200, got %d", rec3.Code)
	}

	// Describe permissions
	rec4 := doTargetRequest(t, h, "DescribeWorkspaceImagePermissions", map[string]any{
		"ImageId": imageID,
	})
	if rec4.Code != http.StatusOK {
		t.Fatalf("describe perms: expected 200, got %d", rec4.Code)
	}

	var permsOut struct {
		ImageId          string           `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
		ImagePermissions []map[string]any `json:"ImagePermissions"`
	}
	decodeJSON(t, rec4.Body.Bytes(), &permsOut)

	if len(permsOut.ImagePermissions) != 1 {
		t.Fatalf("expected 1 permission, got %d", len(permsOut.ImagePermissions))
	}

	// DescribeCustomWorkspaceImageImport
	rec5 := doTargetRequest(t, h, "DescribeCustomWorkspaceImageImport", map[string]any{
		"ImageId": imageID,
	})
	if rec5.Code != http.StatusOK {
		t.Fatalf("describe custom import: expected 200, got %d", rec5.Code)
	}

	// CreateUpdatedWorkspaceImage
	rec6 := doTargetRequest(t, h, "CreateUpdatedWorkspaceImage", map[string]any{
		"SourceImageId": imageID,
		"Name":          "updated",
		"Description":   "updated version",
	})
	if rec6.Code != http.StatusOK {
		t.Fatalf("create updated image: expected 200, got %d", rec6.Code)
	}

	// Delete image
	rec7 := doTargetRequest(t, h, "DeleteWorkspaceImage", map[string]any{
		"ImageId": imageID,
	})
	if rec7.Code != http.StatusOK {
		t.Fatalf("delete image: expected 200, got %d", rec7.Code)
	}
}

// =============================================================================
// Pool tests
// ============================================================================= //nolint:godot // existing issue.
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

// =============================================================================
// Directory tests
// ============================================================================= //nolint:godot // existing issue.
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

// =============================================================================
// Account tests
// ============================================================================= //nolint:godot // existing issue.
func TestDescribeAndModifyAccount(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	// Describe account (defaults)
	rec := doTargetRequest(t, h, "DescribeAccount", map[string]any{})
	if rec.Code != http.StatusOK {
		t.Fatalf("describe account: expected 200, got %d", rec.Code)
	}

	var descOut map[string]string
	decodeJSON(t, rec.Body.Bytes(), &descOut)

	if descOut["DedicatedTenancySupport"] != "ENABLED" {
		t.Fatalf("expected ENABLED, got %s", descOut["DedicatedTenancySupport"])
	}

	// Describe account modifications
	rec2 := doTargetRequest(t, h, "DescribeAccountModifications", map[string]any{})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe modifications: expected 200, got %d", rec2.Code)
	}

	// Modify account
	rec3 := doTargetRequest(t, h, "ModifyAccount", map[string]any{
		"DedicatedTenancyManagementCidrRange": "10.0.0.0/16",
		"DedicatedTenancySupport":             "ENABLED",
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("modify account: expected 200, got %d", rec3.Code)
	}

	// Verify change
	rec4 := doTargetRequest(t, h, "DescribeAccount", map[string]any{})
	var descOut2 map[string]string
	decodeJSON(t, rec4.Body.Bytes(), &descOut2)

	if descOut2["DedicatedTenancyManagementCidrRange"] != "10.0.0.0/16" {
		t.Fatalf("expected 10.0.0.0/16, got %s", descOut2["DedicatedTenancyManagementCidrRange"])
	}

	// Modify endpoint encryption
	rec5 := doTargetRequest(t, h, "ModifyEndpointEncryptionMode", map[string]any{
		"DirectoryId":            "d-test",
		"EndpointEncryptionMode": "FIPS_VALIDATED",
	})
	if rec5.Code != http.StatusOK {
		t.Fatalf("modify endpoint encryption: expected 200, got %d", rec5.Code)
	}
}

// =============================================================================
// Connect Client Add-In tests
// ============================================================================= //nolint:godot // existing issue.
func TestConnectClientAddInCRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name       string
		addInName  string
		resourceID string
		url        string
	}{
		{
			name:       "basic addon",
			addInName:  "MyAddIn",
			resourceID: "d-123",
			url:        "https://example.com",
		},
		{name: "second addon", addInName: "AddIn2", resourceID: "d-456", url: "https://other.com"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateConnectClientAddIn", map[string]any{
				"Name":       tc.addInName,
				"ResourceId": tc.resourceID,
				"URL":        tc.url,
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut map[string]string
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			addInID := createOut["AddInId"]
			if addInID == "" {
				t.Fatal("expected non-empty AddInId")
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeConnectClientAddIns", map[string]any{
				"ResourceId": tc.resourceID,
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				AddIns []map[string]string `json:"AddIns"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.AddIns) != 1 {
				t.Fatalf("expected 1 add-in, got %d", len(descOut.AddIns))
			}

			// Update
			rec3 := doTargetRequest(t, h, "UpdateConnectClientAddIn", map[string]any{
				"AddInId":    addInID,
				"ResourceId": tc.resourceID,
				"Name":       "Updated",
				"URL":        "https://updated.example.com",
			})
			if rec3.Code != http.StatusOK {
				t.Fatalf("update: expected 200, got %d", rec3.Code)
			}

			// Delete
			rec4 := doTargetRequest(t, h, "DeleteConnectClientAddIn", map[string]any{
				"AddInId":    addInID,
				"ResourceId": tc.resourceID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec4.Code)
			}

			// Describe after delete
			rec5 := doTargetRequest(t, h, "DescribeConnectClientAddIns", map[string]any{
				"ResourceId": tc.resourceID,
			})
			var afterDel struct {
				AddIns []any `json:"AddIns"`
			}
			decodeJSON(t, rec5.Body.Bytes(), &afterDel)

			if len(afterDel.AddIns) != 0 {
				t.Fatalf("expected 0 add-ins after delete, got %d", len(afterDel.AddIns))
			}
		})
	}
}

// =============================================================================
// Client Branding tests
// ============================================================================= //nolint:godot // existing issue.
func TestClientBranding(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)
	resourceID := "d-branding-test"

	// Import branding
	rec := doTargetRequest(t, h, "ImportClientBranding", map[string]any{
		"ResourceId": resourceID,
		"DeviceTypeWindows": map[string]any{
			"Logo":        "base64data",
			"SupportLink": "https://support.example.com",
		},
		"DeviceTypeOsx": map[string]any{
			"Logo": "maclogo",
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("import branding: expected 200, got %d: %s", rec.Code, rec.Body)
	}

	// Describe branding
	rec2 := doTargetRequest(t, h, "DescribeClientBranding", map[string]any{
		"ResourceId": resourceID,
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("describe branding: expected 200, got %d", rec2.Code)
	}

	var descOut map[string]any
	decodeJSON(t, rec2.Body.Bytes(), &descOut)

	if descOut["DeviceTypeWindows"] == nil {
		t.Fatal("expected DeviceTypeWindows branding")
	}

	// Delete branding for one platform
	rec3 := doTargetRequest(t, h, "DeleteClientBranding", map[string]any{
		"ResourceId": resourceID,
		"Platforms":  []string{"DeviceTypeWindows"},
	})
	if rec3.Code != http.StatusOK {
		t.Fatalf("delete branding: expected 200, got %d", rec3.Code)
	}

	// Verify Windows branding is gone
	rec4 := doTargetRequest(t, h, "DescribeClientBranding", map[string]any{
		"ResourceId": resourceID,
	})
	var descOut2 map[string]any
	decodeJSON(t, rec4.Body.Bytes(), &descOut2)

	if descOut2["DeviceTypeWindows"] != nil {
		t.Fatal("expected DeviceTypeWindows branding to be deleted")
	}

	if descOut2["DeviceTypeOsx"] == nil {
		t.Fatal("expected DeviceTypeOsx branding to remain")
	}
}

// =============================================================================
// Client Properties tests
// ============================================================================= //nolint:godot // existing issue.
func TestClientProperties(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name             string
		resourceID       string
		reconnectEnabled string
	}{
		{name: "enabled", resourceID: "d-001", reconnectEnabled: "ENABLED"},
		{name: "disabled", resourceID: "d-002", reconnectEnabled: "DISABLED"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Modify
			rec := doTargetRequest(t, h, "ModifyClientProperties", map[string]any{
				"ResourceId": tc.resourceID,
				"ClientProperties": map[string]any{
					"ReconnectEnabled": tc.reconnectEnabled,
				},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("modify: expected 200, got %d", rec.Code)
			}

			// Describe
			rec2 := doTargetRequest(t, h, "DescribeClientProperties", map[string]any{
				"ResourceIds": []string{tc.resourceID},
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("describe: expected 200, got %d", rec2.Code)
			}

			var descOut struct {
				ClientPropertiesList []struct {
					ClientProperties map[string]any `json:"ClientProperties"`
					ResourceId       string         `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
				} `json:"ClientPropertiesList"`
			}
			decodeJSON(t, rec2.Body.Bytes(), &descOut)

			if len(descOut.ClientPropertiesList) != 1 {
				t.Fatalf("expected 1 item, got %d", len(descOut.ClientPropertiesList))
			}

			got, _ := descOut.ClientPropertiesList[0].ClientProperties["ReconnectEnabled"].(string)
			if got != tc.reconnectEnabled {
				t.Fatalf("expected %s, got %s", tc.reconnectEnabled, got)
			}
		})
	}
}

// =============================================================================
// Directory modify ops tests
// ============================================================================= //nolint:godot // existing issue.
func TestDirectoryModifyOps(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "ModifyCertificateBasedAuthProperties",
			op:   "ModifyCertificateBasedAuthProperties",
			body: map[string]any{
				"DirectoryId": "d-cert",
				"CertificateBasedAuthProperties": map[string]any{
					"Status":                  "ENABLED",
					"CertificateAuthorityArn": "arn:aws:acm:us-east-1:123:ca/abc",
				},
			},
		},
		{
			name: "ModifySamlProperties",
			op:   "ModifySamlProperties",
			body: map[string]any{
				"DirectoryId": "d-saml",
				"SamlProperties": map[string]any{
					"Status":        "ENABLED",
					"UserAccessUrl": "https://saml.example.com",
				},
			},
		},
		{
			name: "ModifySelfservicePermissions",
			op:   "ModifySelfservicePermissions",
			body: map[string]any{
				"DirectoryId": "d-selfservice",
				"SelfservicePermissions": map[string]any{
					"RestartWorkspace": "ENABLED",
				},
			},
		},
		{
			name: "ModifyStreamingProperties",
			op:   "ModifyStreamingProperties",
			body: map[string]any{
				"DirectoryId": "d-streaming",
				"StreamingProperties": map[string]any{
					"StreamingExperiencePreferredProtocol": "TCP",
				},
			},
		},
		{
			name: "ModifyWorkspaceAccessProperties",
			op:   "ModifyWorkspaceAccessProperties",
			body: map[string]any{
				"DirectoryId": "d-access",
				"WorkspaceAccessProperties": map[string]any{
					"DeviceTypeWindows": "ALLOW",
				},
			},
		},
		{
			name: "ModifyWorkspaceCreationProperties",
			op:   "ModifyWorkspaceCreationProperties",
			body: map[string]any{
				"DirectoryId": "d-creation",
				"WorkspaceCreationProperties": map[string]any{
					"DefaultOu": "OU=Workspaces,DC=example,DC=com",
				},
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d: %s", tc.op, rec.Code, rec.Body)
			}
		})
	}
}

// =============================================================================
// Account Link tests
// ============================================================================= //nolint:godot // existing issue.
func TestAccountLinkLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name            string
		targetAccountID string
		action          string // "accept" or "reject"
	}{
		{name: "accept link", targetAccountID: "999988887777", action: "accept"},
		{name: "reject link", targetAccountID: "111100002222", action: "reject"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateAccountLinkInvitation", map[string]any{
				"TargetAccountId": tc.targetAccountID,
				"ClientToken":     "tok-123",
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			linkID := createOut.AccountLink["LinkId"]
			if linkID == "" {
				t.Fatal("expected non-empty LinkId")
			}

			if createOut.AccountLink["Status"] != "PENDING_ACCEPTANCE" {
				t.Fatalf("expected PENDING_ACCEPTANCE, got %s", createOut.AccountLink["Status"])
			}

			// GetAccountLink
			rec2 := doTargetRequest(t, h, "GetAccountLink", map[string]any{
				"LinkId": linkID,
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("get: expected 200, got %d", rec2.Code)
			}

			// ListAccountLinks
			rec3 := doTargetRequest(t, h, "ListAccountLinks", map[string]any{})
			if rec3.Code != http.StatusOK {
				t.Fatalf("list: expected 200, got %d", rec3.Code)
			}

			var listOut struct {
				AccountLinks []map[string]string `json:"AccountLinks"`
			}
			decodeJSON(t, rec3.Body.Bytes(), &listOut)

			if len(listOut.AccountLinks) != 1 {
				t.Fatalf("expected 1 link, got %d", len(listOut.AccountLinks))
			}

			// Accept or Reject
			var actionOp string
			var expectedStatus string

			if tc.action == "accept" {
				actionOp = "AcceptAccountLinkInvitation"
				expectedStatus = "LINKED"
			} else {
				actionOp = "RejectAccountLinkInvitation"
				expectedStatus = "REJECTED"
			}

			rec4 := doTargetRequest(t, h, actionOp, map[string]any{
				"LinkId": linkID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d", actionOp, rec4.Code)
			}

			var actionOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec4.Body.Bytes(), &actionOut)

			if actionOut.AccountLink["Status"] != expectedStatus {
				t.Fatalf("expected %s, got %s", expectedStatus, actionOut.AccountLink["Status"])
			}

			// Delete
			rec5 := doTargetRequest(t, h, "DeleteAccountLinkInvitation", map[string]any{
				"LinkId": linkID,
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec5.Code)
			}

			var delOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec5.Body.Bytes(), &delOut)

			if delOut.AccountLink["Status"] != "DELETED" {
				t.Fatalf("expected DELETED, got %s", delOut.AccountLink["Status"])
			}
		})
	}
}

// =============================================================================
// Application association tests
// ============================================================================= //nolint:godot // existing issue.
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

// =============================================================================
// Workspace-level op tests
// ============================================================================= //nolint:godot // existing issue.
func TestWorkspaceLevelOps(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-test"})

	// Create a workspace
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "bob",
				"DirectoryId": "d-test",
				"BundleId":    "wsb-src",
			},
		},
	})
	var wsOut struct {
		PendingRequests []map[string]string `json:"PendingRequests"`
	}
	decodeJSON(t, rec.Body.Bytes(), &wsOut)

	wsID := wsOut.PendingRequests[0]["WorkspaceId"]

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "RestoreWorkspace",
			op:   "RestoreWorkspace",
			body: map[string]any{"WorkspaceId": wsID},
		},
		{
			name: "DescribeWorkspaceSnapshots",
			op:   "DescribeWorkspaceSnapshots",
			body: map[string]any{"WorkspaceId": wsID},
		},
		{
			name: "ListAvailableManagementCidrRanges",
			op:   "ListAvailableManagementCidrRanges",
			body: map[string]any{},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			r := doTargetRequest(t, h, tc.op, tc.body)
			if r.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d: %s", tc.op, r.Code, r.Body)
			}
		})
	}
}
func TestMigrateWorkspace(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-test"})

	// Create workspace
	rec := doTargetRequest(t, h, "CreateWorkspaces", map[string]any{
		"Workspaces": []map[string]any{
			{
				"UserName":    "carol",
				"DirectoryId": "d-test",
				"BundleId":    "wsb-old",
			},
		},
	})
	var wsOut struct {
		PendingRequests []map[string]string `json:"PendingRequests"`
	}
	decodeJSON(t, rec.Body.Bytes(), &wsOut)

	srcID := wsOut.PendingRequests[0]["WorkspaceId"]

	// Migrate
	rec2 := doTargetRequest(t, h, "MigrateWorkspace", map[string]any{
		"SourceWorkspaceId": srcID,
		"BundleId":          "wsb-new",
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("migrate: expected 200, got %d: %s", rec2.Code, rec2.Body)
	}

	var migrateOut map[string]string
	decodeJSON(t, rec2.Body.Bytes(), &migrateOut)

	if migrateOut["SourceWorkspaceId"] != srcID {
		t.Fatalf("expected SourceWorkspaceId=%s, got %s", srcID, migrateOut["SourceWorkspaceId"])
	}

	if migrateOut["TargetWorkspaceId"] == "" {
		t.Fatal("expected non-empty TargetWorkspaceId")
	}

	if migrateOut["TargetWorkspaceId"] == srcID {
		t.Fatal("target should differ from source")
	}
}
func TestCreateStandbyWorkspaces(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	rec := doTargetRequest(t, h, "CreateStandbyWorkspaces", map[string]any{
		"PrimaryRegion": "us-east-1",
		"StandbyWorkspaces": []map[string]any{
			{"PrimaryWorkspaceId": "ws-000001", "DirectoryId": "d-test"},
			{"PrimaryWorkspaceId": "ws-000002", "DirectoryId": "d-test"},
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body)
	}

	var out struct {
		FailedStandbyRequests  []any `json:"FailedStandbyRequests"`
		PendingStandbyRequests []any `json:"PendingStandbyRequests"`
	}
	decodeJSON(t, rec.Body.Bytes(), &out)

	if len(out.FailedStandbyRequests) != 0 {
		t.Fatalf("expected no failures, got %d", len(out.FailedStandbyRequests))
	}

	if len(out.PendingStandbyRequests) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(out.PendingStandbyRequests))
	}
}
func TestListAvailableManagementCidrRanges(t *testing.T) { //nolint:paralleltest // existing issue.
	h, _ := newTestHandlerWithBackend(t)

	rec := doTargetRequest(t, h, "ListAvailableManagementCidrRanges", map[string]any{})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var out struct {
		ManagementCidrRanges []string `json:"ManagementCidrRanges"`
	}
	decodeJSON(t, rec.Body.Bytes(), &out)

	if len(out.ManagementCidrRanges) == 0 {
		t.Fatal("expected non-empty CIDR ranges")
	}
}
