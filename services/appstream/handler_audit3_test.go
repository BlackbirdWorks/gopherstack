package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// helpers

func createAppBlock(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateAppBlock", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createAppBlockBuilder(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateAppBlockBuilder", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createApplication(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       name,
		"LaunchPath": "/app/" + name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createImage(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateImportedImage", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createImageBuilder(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateImageBuilder", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createUser(t *testing.T, h *appstream.Handler, userName string) {
	t.Helper()
	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           userName,
		"Email":              userName + "@example.com",
		"AuthenticationType": "USERPOOL",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestAppStream_AppBlocks covers AppBlock CRUD.
func TestAppStream_AppBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateAppBlock returns app block with ARN",
			action:   "CreateAppBlock",
			body:     map[string]any{"Name": "my-block", "Description": "test"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ab := resp["AppBlock"].(map[string]any)
				assert.Equal(t, "my-block", ab["Name"])
				assert.Contains(t, ab["Arn"], ":app-block/my-block")
				assert.Equal(t, "INACTIVE", ab["State"])
			},
		},
		{
			name:   "CreateAppBlock duplicate returns error",
			action: "CreateAppBlock",
			setup: func(h *appstream.Handler) {
				createAppBlock(t, h, "dup-block")
			},
			body:     map[string]any{"Name": "dup-block"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeAppBlocks returns all blocks",
			action: "DescribeAppBlocks",
			setup: func(h *appstream.Handler) {
				createAppBlock(t, h, "blk-a")
				createAppBlock(t, h, "blk-b")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				blocks := resp["AppBlocks"].([]any)
				assert.Len(t, blocks, 2)
			},
		},
		{
			name:   "DeleteAppBlock removes block",
			action: "DeleteAppBlock",
			setup: func(h *appstream.Handler) {
				createAppBlock(t, h, "del-block")
			},
			body:     map[string]any{"Name": "del-block"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteAppBlock unknown returns error",
			action:   "DeleteAppBlock",
			body:     map[string]any{"Name": "no-such"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_AppBlockBuilders covers AppBlockBuilder CRUD + Start/Stop.
func TestAppStream_AppBlockBuilders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateAppBlockBuilder returns builder",
			action: "CreateAppBlockBuilder",
			body: map[string]any{
				"Name":         "my-builder",
				"InstanceType": "stream.standard.medium",
				"Platform":     "WINDOWS_SERVER_2019",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				bb := resp["AppBlockBuilder"].(map[string]any)
				assert.Equal(t, "my-builder", bb["Name"])
				assert.Equal(t, "STOPPED", bb["State"])
			},
		},
		{
			name:     "CreateAppBlockBuilder missing InstanceType returns error",
			action:   "CreateAppBlockBuilder",
			body:     map[string]any{"Name": "no-instance"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "StartAppBlockBuilder transitions to RUNNING",
			action: "StartAppBlockBuilder",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "start-builder")
			},
			body:     map[string]any{"Name": "start-builder"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				bb := resp["AppBlockBuilder"].(map[string]any)
				assert.Equal(t, "RUNNING", bb["State"])
			},
		},
		{
			name:   "StopAppBlockBuilder transitions to STOPPED",
			action: "StopAppBlockBuilder",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "stop-builder")
				rec := doRequest(t, h, "StartAppBlockBuilder", map[string]any{"Name": "stop-builder"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "stop-builder"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				bb := resp["AppBlockBuilder"].(map[string]any)
				assert.Equal(t, "STOPPED", bb["State"])
			},
		},
		{
			name:   "UpdateAppBlockBuilder changes description",
			action: "UpdateAppBlockBuilder",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "upd-builder")
			},
			body:     map[string]any{"Name": "upd-builder", "Description": "updated"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				bb := resp["AppBlockBuilder"].(map[string]any)
				assert.Equal(t, "updated", bb["Description"])
			},
		},
		{
			name:   "CreateAppBlockBuilderStreamingURL returns URL",
			action: "CreateAppBlockBuilderStreamingURL",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "url-builder")
			},
			body:     map[string]any{"AppBlockBuilderName": "url-builder"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.NotEmpty(t, resp["StreamingURL"])
			},
		},
		{
			name:   "AssociateAppBlockBuilderAppBlock creates association",
			action: "AssociateAppBlockBuilderAppBlock",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "assoc-builder")
				createAppBlock(t, h, "assoc-block")
			},
			body: map[string]any{
				"AppBlockBuilderName": "assoc-builder",
				"AppBlockArn":         "assoc-block",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeAppBlockBuilderAppBlockAssociations lists association",
			action: "DescribeAppBlockBuilderAppBlockAssociations",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "list-builder")
				createAppBlock(t, h, "list-block")
				rec := doRequest(t, h, "AssociateAppBlockBuilderAppBlock", map[string]any{
					"AppBlockBuilderName": "list-builder",
					"AppBlockArn":         "list-block",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"AppBlockBuilderName": "list-builder"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assocs := resp["AppBlockBuilderAppBlockAssociations"].([]any)
				assert.Len(t, assocs, 1)
			},
		},
		{
			name:   "DeleteAppBlockBuilder removes builder",
			action: "DeleteAppBlockBuilder",
			setup: func(h *appstream.Handler) {
				createAppBlockBuilder(t, h, "del-builder")
			},
			body:     map[string]any{"Name": "del-builder"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Applications covers Application CRUD and fleet associations.
func TestAppStream_Applications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateApplication returns application with ARN",
			action: "CreateApplication",
			body: map[string]any{
				"Name":       "my-app",
				"LaunchPath": "/app/my-app",
				"Platforms":  []string{"WINDOWS_SERVER_2019"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				app := resp["Application"].(map[string]any)
				assert.Equal(t, "my-app", app["Name"])
				assert.Contains(t, app["Arn"], ":application/my-app")
			},
		},
		{
			name:   "CreateApplication duplicate returns error",
			action: "CreateApplication",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "dup-app")
			},
			body:     map[string]any{"Name": "dup-app", "LaunchPath": "/x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeApplications lists all",
			action: "DescribeApplications",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "app-a")
				createApplication(t, h, "app-b")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				apps := resp["Applications"].([]any)
				assert.Len(t, apps, 2)
			},
		},
		{
			name:   "UpdateApplication changes display name",
			action: "UpdateApplication",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "upd-app")
			},
			body:     map[string]any{"Name": "upd-app", "DisplayName": "Updated App"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				app := resp["Application"].(map[string]any)
				assert.Equal(t, "Updated App", app["DisplayName"])
			},
		},
		{
			name:     "DeleteApplication removes application",
			action:   "DeleteApplication",
			setup:    func(h *appstream.Handler) { createApplication(t, h, "del-app") },
			body:     map[string]any{"Name": "del-app"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DescribeAppLicenseUsage returns empty",
			action:   "DescribeAppLicenseUsage",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateApplicationFleet creates link",
			action: "AssociateApplicationFleet",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "link-app")
				createFleet(t, h, "link-fleet")
			},
			body: map[string]any{
				"ApplicationArn": "link-app",
				"FleetName":      "link-fleet",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DisassociateApplicationFleet removes link",
			action: "DisassociateApplicationFleet",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "dis-app")
				createFleet(t, h, "dis-fleet")
				rec := doRequest(t, h, "AssociateApplicationFleet", map[string]any{
					"ApplicationArn": "dis-app",
					"FleetName":      "dis-fleet",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationArn": "dis-app",
				"FleetName":      "dis-fleet",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeApplicationFleetAssociations lists links",
			action: "DescribeApplicationFleetAssociations",
			setup: func(h *appstream.Handler) {
				createApplication(t, h, "lst-app")
				createFleet(t, h, "lst-fleet")
				rec := doRequest(t, h, "AssociateApplicationFleet", map[string]any{
					"ApplicationArn": "lst-app",
					"FleetName":      "lst-fleet",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"ApplicationArn": "lst-app"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assocs := resp["ApplicationFleetAssociations"].([]any)
				assert.Len(t, assocs, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Entitlements covers Entitlement CRUD and application associations.
func TestAppStream_Entitlements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateEntitlement returns entitlement",
			action: "CreateEntitlement",
			body: map[string]any{
				"Name":          "my-ent",
				"StackName":     "stk",
				"AppVisibility": "ALL",
				"Attributes": []map[string]string{
					{"Name": "department", "Value": "eng"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ent := resp["Entitlement"].(map[string]any)
				assert.Equal(t, "my-ent", ent["Name"])
				assert.Equal(t, "ALL", ent["AppVisibility"])
			},
		},
		{
			name:   "DescribeEntitlements lists by stack",
			action: "DescribeEntitlements",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name":          "ent-1",
					"StackName":     "stk-1",
					"AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "stk-1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ents := resp["Entitlements"].([]any)
				assert.Len(t, ents, 1)
			},
		},
		{
			name:   "UpdateEntitlement changes description",
			action: "UpdateEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "upd-ent", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"Name": "upd-ent", "StackName": "stk", "Description": "updated",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ent := resp["Entitlement"].(map[string]any)
				assert.Equal(t, "updated", ent["Description"])
			},
		},
		{
			name:   "DeleteEntitlement removes it",
			action: "DeleteEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "del-ent", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "del-ent", "StackName": "stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateApplicationToEntitlement adds app",
			action: "AssociateApplicationToEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-assoc", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationIdentifier": "app-001",
				"EntitlementName":       "ent-assoc",
				"StackName":             "stk",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListEntitledApplications returns apps",
			action: "ListEntitledApplications",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-list", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "AssociateApplicationToEntitlement", map[string]any{
					"ApplicationIdentifier": "app-x",
					"EntitlementName":       "ent-list",
					"StackName":             "stk",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"EntitlementName": "ent-list", "StackName": "stk"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				apps := resp["EntitledApplications"].([]any)
				assert.Len(t, apps, 1)
			},
		},
		{
			name:   "DisassociateApplicationFromEntitlement removes app",
			action: "DisassociateApplicationFromEntitlement",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateEntitlement", map[string]any{
					"Name": "ent-dis", "StackName": "stk", "AppVisibility": "ALL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "AssociateApplicationToEntitlement", map[string]any{
					"ApplicationIdentifier": "app-y",
					"EntitlementName":       "ent-dis",
					"StackName":             "stk",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ApplicationIdentifier": "app-y",
				"EntitlementName":       "ent-dis",
				"StackName":             "stk",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_DirectoryConfigs covers DirectoryConfig CRUD.
func TestAppStream_DirectoryConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateDirectoryConfig returns config",
			action: "CreateDirectoryConfig",
			body: map[string]any{
				"DirectoryName":                        "corp.example.com",
				"OrganizationalUnitDistinguishedNames": []string{"OU=AppStream,DC=corp,DC=example,DC=com"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				dc := resp["DirectoryConfig"].(map[string]any)
				assert.Equal(t, "corp.example.com", dc["DirectoryName"])
			},
		},
		{
			name:   "DescribeDirectoryConfigs lists all",
			action: "DescribeDirectoryConfigs",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "a.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				dcs := resp["DirectoryConfigs"].([]any)
				assert.Len(t, dcs, 1)
			},
		},
		{
			name:   "UpdateDirectoryConfig updates OUs",
			action: "UpdateDirectoryConfig",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "upd.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"DirectoryName":                        "upd.com",
				"OrganizationalUnitDistinguishedNames": []string{"OU=New,DC=upd,DC=com"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDirectoryConfig removes it",
			action: "DeleteDirectoryConfig",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "del.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"DirectoryName": "del.com"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDirectoryConfig unknown returns error",
			action:   "DeleteDirectoryConfig",
			body:     map[string]any{"DirectoryName": "no-such.com"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Images covers Image CRUD, copy, permissions.
func TestAppStream_Images(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateImportedImage returns image",
			action:   "CreateImportedImage",
			body:     map[string]any{"Name": "my-img"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				img := resp["Image"].(map[string]any)
				assert.Equal(t, "my-img", img["Name"])
				assert.Equal(t, "AVAILABLE", img["State"])
			},
		},
		{
			name:   "DescribeImages lists all",
			action: "DescribeImages",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "img-a")
				createImage(t, h, "img-b")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				imgs := resp["Images"].([]any)
				assert.Len(t, imgs, 2)
			},
		},
		{
			name:   "CopyImage creates copy",
			action: "CopyImage",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "src-img")
			},
			body: map[string]any{
				"SourceImageName":      "src-img",
				"DestinationImageName": "dst-img",
				"DestinationRegion":    "us-west-2",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Equal(t, "dst-img", resp["DestinationImageName"])
			},
		},
		{
			name:   "CreateUpdatedImage creates new version",
			action: "CreateUpdatedImage",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "base-img")
			},
			body: map[string]any{
				"ExistingImageName": "base-img",
				"NewImageName":      "updated-img",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteImage removes image",
			action: "DeleteImage",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "rm-img")
			},
			body:     map[string]any{"Name": "rm-img"},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateImagePermissions sets perms",
			action: "UpdateImagePermissions",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "perm-img")
			},
			body: map[string]any{
				"Name":            "perm-img",
				"SharedAccountId": "111111111111",
				"ImagePermissions": map[string]any{
					"AllowFleet": true, "AllowImageBuilder": false,
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeImagePermissions returns perms",
			action: "DescribeImagePermissions",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "desc-perm-img")
				rec := doRequest(t, h, "UpdateImagePermissions", map[string]any{
					"Name": "desc-perm-img", "SharedAccountId": "222222222222",
					"ImagePermissions": map[string]any{"AllowFleet": true},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "desc-perm-img"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				perms := resp["SharedImagePermissions"].([]any)
				assert.Len(t, perms, 1)
			},
		},
		{
			name:   "DeleteImagePermissions removes perms",
			action: "DeleteImagePermissions",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "del-perm-img")
				rec := doRequest(t, h, "UpdateImagePermissions", map[string]any{
					"Name": "del-perm-img", "SharedAccountId": "333333333333",
					"ImagePermissions": map[string]any{"AllowFleet": true},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "del-perm-img", "SharedAccountId": "333333333333"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_ImageBuilders covers ImageBuilder CRUD, Start/Stop, software associations.
func TestAppStream_ImageBuilders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateImageBuilder returns builder in STOPPED state",
			action: "CreateImageBuilder",
			body: map[string]any{
				"Name":         "my-ib",
				"InstanceType": "stream.standard.medium",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ib := resp["ImageBuilder"].(map[string]any)
				assert.Equal(t, "STOPPED", ib["State"])
			},
		},
		{
			name:   "StartImageBuilder transitions to RUNNING",
			action: "StartImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "start-ib")
			},
			body:     map[string]any{"Name": "start-ib"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ib := resp["ImageBuilder"].(map[string]any)
				assert.Equal(t, "RUNNING", ib["State"])
				assert.NotEmpty(t, resp["StreamingURL"])
			},
		},
		{
			name:   "StopImageBuilder transitions to STOPPED",
			action: "StopImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "stop-ib")
				rec := doRequest(t, h, "StartImageBuilder", map[string]any{"Name": "stop-ib"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "stop-ib"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				ib := resp["ImageBuilder"].(map[string]any)
				assert.Equal(t, "STOPPED", ib["State"])
			},
		},
		{
			name:   "CreateImageBuilderStreamingURL returns URL",
			action: "CreateImageBuilderStreamingURL",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "url-ib")
			},
			body:     map[string]any{"Name": "url-ib"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.NotEmpty(t, resp["StreamingURL"])
			},
		},
		{
			name:   "AssociateSoftwareToImageBuilder adds software",
			action: "AssociateSoftwareToImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "sw-ib")
			},
			body: map[string]any{
				"ImageBuilderName": "sw-ib",
				"Software":         []string{"pkg-a", "pkg-b"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeSoftwareAssociations lists software",
			action: "DescribeSoftwareAssociations",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "list-sw-ib")
				rec := doRequest(t, h, "AssociateSoftwareToImageBuilder", map[string]any{
					"ImageBuilderName": "list-sw-ib",
					"Software":         []string{"pkg-x"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"ImageBuilderName": "list-sw-ib"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assocs := resp["SoftwareAssociations"].([]any)
				assert.Len(t, assocs, 1)
			},
		},
		{
			name:   "DisassociateSoftwareFromImageBuilder removes software",
			action: "DisassociateSoftwareFromImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "dis-sw-ib")
				rec := doRequest(t, h, "AssociateSoftwareToImageBuilder", map[string]any{
					"ImageBuilderName": "dis-sw-ib",
					"Software":         []string{"pkg-z"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"ImageBuilderName": "dis-sw-ib",
				"Software":         []string{"pkg-z"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "StartSoftwareDeploymentToImageBuilder succeeds",
			action: "StartSoftwareDeploymentToImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "deploy-ib")
			},
			body:     map[string]any{"ImageBuilderName": "deploy-ib"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteImageBuilder removes builder",
			action: "DeleteImageBuilder",
			setup: func(h *appstream.Handler) {
				createImageBuilder(t, h, "del-ib")
			},
			body:     map[string]any{"Name": "del-ib"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_ExportImageTasks covers export task lifecycle.
func TestAppStream_ExportImageTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateExportImageTask returns task ID",
			action: "CreateExportImageTask",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "exp-img")
			},
			body: map[string]any{
				"Name": "exp-img",
				"S3Destination": map[string]any{
					"S3Bucket": "my-bucket",
					"S3Key":    "exports/",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.NotEmpty(t, resp["ExportImageTaskId"])
			},
		},
		{
			name:   "GetExportImageTask retrieves task",
			action: "GetExportImageTask",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "get-exp-img")
				rec := doRequest(t, h, "CreateExportImageTask", map[string]any{
					"Name":          "get-exp-img",
					"S3Destination": map[string]any{"S3Bucket": "b", "S3Key": "k"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))
				_ = r // round-trip tested in TestAppStream_GetExportImageTask
			},
			body:     nil,
			wantCode: http.StatusOK, // skipped below — special case
		},
		{
			name:   "ListExportImageTasks returns all",
			action: "ListExportImageTasks",
			setup: func(h *appstream.Handler) {
				createImage(t, h, "lst-exp-img")
				rec := doRequest(t, h, "CreateExportImageTask", map[string]any{
					"Name":          "lst-exp-img",
					"S3Destination": map[string]any{"S3Bucket": "b", "S3Key": "k"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				tasks := resp["ExportImageTasks"].([]any)
				assert.Len(t, tasks, 1)
			},
		},
	}

	for _, tc := range tests {
		if tc.body == nil {
			continue // skip special cases
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_GetExportImageTask covers the round-trip for GetExportImageTask.
func TestAppStream_GetExportImageTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createImage(t, h, "rt-img")

	rec := doRequest(t, h, "CreateExportImageTask", map[string]any{
		"Name": "rt-img", "S3Destination": map[string]any{"S3Bucket": "b", "S3Key": "k"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	taskID := createResp["ExportImageTaskId"].(string)

	rec2 := doRequest(t, h, "GetExportImageTask", map[string]any{"ExportImageTaskId": taskID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))
	task := getResp["ExportImageTask"].(map[string]any)
	assert.Equal(t, taskID, task["ExportImageTaskId"])
}

// TestAppStream_UsageReports covers UsageReportSubscription lifecycle.
func TestAppStream_UsageReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateUsageReportSubscription returns subscription",
			action: "CreateUsageReportSubscription",
			body: map[string]any{
				"S3BucketName": "my-usage-bucket",
				"Schedule":     "DAILY",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Equal(t, "my-usage-bucket", resp["S3BucketName"])
			},
		},
		{
			name:   "DescribeUsageReportSubscriptions returns subscription",
			action: "DescribeUsageReportSubscriptions",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{
					"S3BucketName": "bucket-a",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				subs := resp["Subscriptions"].([]any)
				assert.Len(t, subs, 1)
			},
		},
		{
			name:   "DeleteUsageReportSubscription removes it",
			action: "DeleteUsageReportSubscription",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{
					"S3BucketName": "bucket-b",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUsageReportSubscription when none returns error",
			action:   "DeleteUsageReportSubscription",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Themes covers Theme CRUD.
func TestAppStream_Themes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateThemeForStack returns theme",
			action: "CreateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "theme-stk")
			},
			body:     map[string]any{"StackName": "theme-stk"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				th := resp["Theme"].(map[string]any)
				assert.Equal(t, "theme-stk", th["StackName"])
				assert.Equal(t, "ENABLED", th["State"])
			},
		},
		{
			name:   "DescribeThemeForStack returns theme",
			action: "DescribeThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "desc-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "desc-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "desc-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateThemeForStack returns theme",
			action: "UpdateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "upd-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "upd-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "upd-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteThemeForStack removes theme",
			action: "DeleteThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "del-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "del-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "del-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DescribeThemeForStack unknown returns error",
			action:   "DescribeThemeForStack",
			body:     map[string]any{"StackName": "no-such"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Users covers User CRUD and enable/disable.
func TestAppStream_Users(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateUser returns OK",
			action: "CreateUser",
			body: map[string]any{
				"UserName":           "alice@example.com",
				"Email":              "alice@example.com",
				"AuthenticationType": "USERPOOL",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "CreateUser duplicate returns error",
			action: "CreateUser",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "dup-user")
			},
			body: map[string]any{
				"UserName":           "dup-user",
				"Email":              "dup@example.com",
				"AuthenticationType": "USERPOOL",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeUsers lists all",
			action: "DescribeUsers",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "usr-a")
				createUser(t, h, "usr-b")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				users := resp["Users"].([]any)
				assert.Len(t, users, 2)
			},
		},
		{
			name:   "DisableUser disables user",
			action: "DisableUser",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "dis-usr")
			},
			body:     map[string]any{"UserName": "dis-usr", "AuthenticationType": "USERPOOL"},
			wantCode: http.StatusOK,
		},
		{
			name:   "EnableUser re-enables user",
			action: "EnableUser",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "en-usr")
				rec := doRequest(t, h, "DisableUser", map[string]any{
					"UserName": "en-usr", "AuthenticationType": "USERPOOL",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"UserName": "en-usr", "AuthenticationType": "USERPOOL"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteUser removes user",
			action: "DeleteUser",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "del-usr")
			},
			body:     map[string]any{"UserName": "del-usr", "AuthenticationType": "USERPOOL"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUser unknown returns error",
			action:   "DeleteUser",
			body:     map[string]any{"UserName": "no-such", "AuthenticationType": "USERPOOL"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_UserStackAssociations covers batch user-stack association ops.
func TestAppStream_UserStackAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "BatchAssociateUserStack links user to stack",
			action: "BatchAssociateUserStack",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "ba-user")
			},
			body: map[string]any{
				"UserStackAssociations": []map[string]any{
					{
						"UserName":           "ba-user",
						"StackName":          "any-stack",
						"AuthenticationType": "USERPOOL",
					},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				errs := resp["Errors"].([]any)
				assert.Empty(t, errs)
			},
		},
		{
			name:   "BatchAssociateUserStack unknown user returns error entry",
			action: "BatchAssociateUserStack",
			body: map[string]any{
				"UserStackAssociations": []map[string]any{
					{
						"UserName":           "ghost",
						"StackName":          "any-stack",
						"AuthenticationType": "USERPOOL",
					},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				errs := resp["Errors"].([]any)
				assert.Len(t, errs, 1)
			},
		},
		{
			name:   "DescribeUserStackAssociations lists links",
			action: "DescribeUserStackAssociations",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "list-ba-user")
				rec := doRequest(t, h, "BatchAssociateUserStack", map[string]any{
					"UserStackAssociations": []map[string]any{
						{
							"UserName":           "list-ba-user",
							"StackName":          "list-stk",
							"AuthenticationType": "USERPOOL",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "list-stk"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assocs := resp["UserStackAssociations"].([]any)
				assert.Len(t, assocs, 1)
			},
		},
		{
			name:   "BatchDisassociateUserStack removes links",
			action: "BatchDisassociateUserStack",
			setup: func(h *appstream.Handler) {
				createUser(t, h, "dis-ba-user")
				rec := doRequest(t, h, "BatchAssociateUserStack", map[string]any{
					"UserStackAssociations": []map[string]any{
						{
							"UserName": "dis-ba-user", "StackName": "dis-stk",
							"AuthenticationType": "USERPOOL",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"UserStackAssociations": []map[string]any{
					{
						"UserName":           "dis-ba-user",
						"StackName":          "dis-stk",
						"AuthenticationType": "USERPOOL",
					},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_Sessions covers session lifecycle and streaming URLs.
func TestAppStream_Sessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateStreamingURL creates session and returns URL",
			action: "CreateStreamingURL",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "sess-stk")
				createFleet(t, h, "sess-fleet")
			},
			body: map[string]any{
				"StackName": "sess-stk",
				"FleetName": "sess-fleet",
				"UserId":    "user-1",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.NotEmpty(t, resp["StreamingURL"])
			},
		},
		{
			name:   "DescribeSessions returns sessions",
			action: "DescribeSessions",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "desc-sess-stk")
				createFleet(t, h, "desc-sess-fleet")
				rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
					"StackName": "desc-sess-stk",
					"FleetName": "desc-sess-fleet",
					"UserId":    "user-2",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"StackName": "desc-sess-stk",
				"FleetName": "desc-sess-fleet",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				sessions := resp["Sessions"].([]any)
				assert.Len(t, sessions, 1)
			},
		},
		{
			name:   "ExpireSession removes session",
			action: "ExpireSession",
			setup:  func(h *appstream.Handler) {}, //nolint:revive // existing issue.
			body:   nil,
			// Special case handled below
			wantCode: http.StatusOK,
		},
		{
			name:   "DrainSessionInstance removes session",
			action: "DrainSessionInstance",
			setup:  func(h *appstream.Handler) {}, //nolint:revive // existing issue.
			body:   nil,
			// Special case handled below
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		if tc.body == nil {
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_ExpireSession and DrainSessionInstance with real session IDs.
func TestAppStream_SessionLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createStack(t, h, "lc-stk")
	createFleet(t, h, "lc-fleet")

	rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "lc-stk",
		"FleetName": "lc-fleet",
		"UserId":    "user-lc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe sessions to get the session ID.
	rec2 := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "lc-stk",
		"FleetName": "lc-fleet",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	sessions := descResp["Sessions"].([]any)
	require.Len(t, sessions, 1)
	sessionID := sessions[0].(map[string]any)["Id"].(string)

	// Expire the session.
	rec3 := doRequest(t, h, "ExpireSession", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Session gone — ExpireSession again should fail.
	rec4 := doRequest(t, h, "ExpireSession", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusBadRequest, rec4.Code)
}

func TestAppStream_DrainSessionInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createStack(t, h, "drain-stk")
	createFleet(t, h, "drain-fleet")

	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "drain-stk", "FleetName": "drain-fleet", "UserId": "user-drain",
	})

	rec := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "drain-stk", "FleetName": "drain-fleet",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	sessions := descResp["Sessions"].([]any)
	require.Len(t, sessions, 1)
	sessionID := sessions[0].(map[string]any)["Id"].(string)

	rec2 := doRequest(t, h, "DrainSessionInstance", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusOK, rec2.Code)
}
