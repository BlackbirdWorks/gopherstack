package codeconnections_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestCreateSyncConfiguration exercises the CreateSyncConfiguration handler.
func TestCreateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
		body        func(linkID string) map[string]any
		name        string
		wantStatus  int
		wantSync    bool
	}{
		{
			name: "success",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "my-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			body: func(linkID string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusOK,
			wantSync:   true,
		},
		{
			name:        "missing_branch",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "missing_config_file",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "missing_role_arn",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "CreateSyncConfiguration", tt.body(linkID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "main", cfg["Branch"])
				assert.Equal(t, "config.yaml", cfg["ConfigFile"])
				assert.Equal(t, "my-stack", cfg["ResourceName"])
				assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
			}
		})
	}
}

// TestDeleteSyncConfiguration exercises the DeleteSyncConfiguration handler.
func TestDeleteSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "my-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": "my-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestSyncConfigurationRoundTrip verifies create/delete round-trip for SyncConfiguration.
func TestSyncConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	connArn := createConn(t, h, "my-conn", "GitHub")
	linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

	createRec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "my-stack",
		"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	resp := parseResp(t, createRec)
	cfg, ok := resp["SyncConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-org", cfg["OwnerId"])
	assert.Equal(t, "my-repo", cfg["RepositoryName"])
	assert.Equal(t, "GitHub", cfg["ProviderType"])

	// Verify resource sync status works.
	syncRec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, syncRec.Code)

	// Delete and verify gone.
	delRec := doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Resource sync status should now return not found.
	afterDelRec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, afterDelRec.Code)
}

// TestSyncTypeValidation verifies invalid sync types are rejected.
func TestSyncTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		syncType   string
		name       string
		wantStatus int
	}{
		{name: "valid_cfn_stack_sync", syncType: "CFN_STACK_SYNC", wantStatus: http.StatusOK},
		{name: "invalid_sync_type", syncType: "INVALID_SYNC", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			connArn := createConn(t, h, "conn-synctype", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "config.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "my-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/role",
				"SyncType":         tt.syncType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCC_SyncConfiguration_CRUD exercises the full SyncConfiguration and sync-blocker
// handler surface end to end.
func TestCC_SyncConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	connArn := createConn(t, h, "test-conn", "GitHub")
	linkID := createRepositoryLink(t, h, connArn, "owner", "repo")

	// CreateSyncConfiguration
	rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       ".aws/sync.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "my-resource",
		"RoleArn":          "arn:aws:iam::123456789012:role/SyncRole",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetSyncConfiguration
	rec = doJSON(t, h, "GetSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListSyncConfigurations
	rec = doJSON(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateSyncConfiguration
	rec = doJSON(t, h, "UpdateSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
		"Branch":       "dev",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSyncBlockerSummary
	rec = doJSON(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListRepositorySyncDefinitions
	rec = doJSON(t, h, "ListRepositorySyncDefinitions", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSyncConfiguration
	rec = doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListSyncConfigurationsPagination verifies MaxResults/NextToken pagination.
func TestListSyncConfigurationsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfgCount   int
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "first_page",
			cfgCount:   3,
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
		{
			name:       "all_results",
			cfgCount:   2,
			maxResults: 10,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "sync-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			for i := range tt.cfgCount {
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "resource-" + strconv.Itoa(i),
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"RepositoryLinkId": linkID,
			}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doJSON(t, h, "ListSyncConfigurations", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			cfgs, ok := resp["SyncConfigurations"].([]any)
			require.True(t, ok)
			assert.Len(t, cfgs, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// TestGetSyncConfiguration exercises the GetSyncConfiguration handler.
func TestGetSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantBranch string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
			wantBranch: "feature-branch",
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "cfg-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "feature-branch",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "get-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "get-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantBranch, cfg["Branch"])
				assert.Equal(t, "my-org", cfg["OwnerId"])
				assert.Equal(t, "my-repo", cfg["RepositoryName"])
				assert.Equal(t, "GitHub", cfg["ProviderType"])
			}
		})
	}
}

// TestUpdateSyncConfiguration exercises UpdateSyncConfiguration.
func TestUpdateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newBranch  string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_updates_branch",
			preCreate:  true,
			newBranch:  "release",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			newBranch:  "release",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "upd-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "upd-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "UpdateSyncConfiguration", map[string]any{
				"ResourceName": "upd-stack",
				"SyncType":     "CFN_STACK_SYNC",
				"Branch":       tt.newBranch,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.newBranch, cfg["Branch"])
			}
		})
	}
}

// TestSyncConfigurationPublishDeploymentStatus verifies that PublishDeploymentStatus is
// stored and returned in GetSyncConfiguration and CreateSyncConfiguration responses.
// Real AWS CodeConnections uses this field to control deployment status publishing.
func TestSyncConfigurationPublishDeploymentStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
		want   string
	}{
		{name: "enabled", status: "ENABLED", want: "ENABLED"},
		{name: "disabled", status: "DISABLED", want: "DISABLED"},
		{name: "omitted", status: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "pds-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "pds-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}
			if tt.status != "" {
				body["PublishDeploymentStatus"] = tt.status
			}

			rec := doJSON(t, h, "CreateSyncConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)

			if tt.want != "" {
				assert.Equal(t, tt.want, cfg["PublishDeploymentStatus"])
			} else {
				v, _ := cfg["PublishDeploymentStatus"].(string)
				assert.Empty(t, v)
			}

			// Verify via GetSyncConfiguration
			rec = doJSON(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "pds-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			getCfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			if tt.want != "" {
				assert.Equal(t, tt.want, getCfg["PublishDeploymentStatus"])
			}
		})
	}
}

// TestSyncConfigurationTriggerResourceUpdateOn verifies TriggerResourceUpdateOn is
// stored and returned.
func TestSyncConfigurationTriggerResourceUpdateOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger string
		want    string
	}{
		{name: "any_change", trigger: "ANY_CHANGE", want: "ANY_CHANGE"},
		{name: "file_change", trigger: "FILE_CHANGE", want: "FILE_CHANGE"},
		{name: "omitted", trigger: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "tru-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "tru-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}
			if tt.trigger != "" {
				body["TriggerResourceUpdateOn"] = tt.trigger
			}

			rec := doJSON(t, h, "CreateSyncConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)

			if tt.want != "" {
				assert.Equal(t, tt.want, cfg["TriggerResourceUpdateOn"])
			} else {
				v, _ := cfg["TriggerResourceUpdateOn"].(string)
				assert.Empty(t, v)
			}
		})
	}
}

// TestUpdateSyncConfigurationPublishDeploymentStatus verifies that
// UpdateSyncConfiguration can update PublishDeploymentStatus.
func TestUpdateSyncConfigurationPublishDeploymentStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial string
		updated string
		want    string
	}{
		{name: "enable_to_disable", initial: "ENABLED", updated: "DISABLED", want: "DISABLED"},
		{name: "omit_update_preserves", initial: "ENABLED", updated: "", want: "ENABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "upd-pds-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":                  "main",
				"ConfigFile":              "sync.yaml",
				"RepositoryLinkId":        linkID,
				"ResourceName":            "upd-pds-stack",
				"RoleArn":                 "arn:aws:iam::123456789012:role/r",
				"SyncType":                "CFN_STACK_SYNC",
				"PublishDeploymentStatus": tt.initial,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			updBody := map[string]any{
				"ResourceName": "upd-pds-stack",
				"SyncType":     "CFN_STACK_SYNC",
			}
			if tt.updated != "" {
				updBody["PublishDeploymentStatus"] = tt.updated
			}

			rec = doJSON(t, h, "UpdateSyncConfiguration", updBody)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			assert.Equal(t, tt.want, cfg["PublishDeploymentStatus"])
		})
	}
}

// TestSyncConfigurationPullRequestComment verifies that PullRequestComment is
// stored and returned in CreateSyncConfiguration/GetSyncConfiguration
// responses. Real AWS CodeConnections (aws-sdk-go-v2/service/codeconnections@
// v1.10.22 types.SyncConfiguration/CreateSyncConfigurationInput) has this
// field; it was previously entirely unimplemented in this service.
func TestSyncConfigurationPullRequestComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{name: "enabled", comment: "ENABLED", want: "ENABLED"},
		{name: "disabled", comment: "DISABLED", want: "DISABLED"},
		{name: "omitted", comment: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "prc-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "prc-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}
			if tt.comment != "" {
				body["PullRequestComment"] = tt.comment
			}

			rec := doJSON(t, h, "CreateSyncConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)

			if tt.want != "" {
				assert.Equal(t, tt.want, cfg["PullRequestComment"])
			} else {
				v, _ := cfg["PullRequestComment"].(string)
				assert.Empty(t, v)
			}

			// Verify via GetSyncConfiguration.
			rec = doJSON(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "prc-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			getCfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			if tt.want != "" {
				assert.Equal(t, tt.want, getCfg["PullRequestComment"])
			}
		})
	}
}

// TestUpdateSyncConfigurationPullRequestComment verifies that
// UpdateSyncConfiguration can update PullRequestComment.
func TestUpdateSyncConfigurationPullRequestComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial string
		updated string
		want    string
	}{
		{name: "enable_to_disable", initial: "ENABLED", updated: "DISABLED", want: "DISABLED"},
		{name: "omit_update_preserves", initial: "ENABLED", updated: "", want: "ENABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "upd-prc-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":             "main",
				"ConfigFile":         "sync.yaml",
				"RepositoryLinkId":   linkID,
				"ResourceName":       "upd-prc-stack",
				"RoleArn":            "arn:aws:iam::123456789012:role/r",
				"SyncType":           "CFN_STACK_SYNC",
				"PullRequestComment": tt.initial,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			updBody := map[string]any{
				"ResourceName": "upd-prc-stack",
				"SyncType":     "CFN_STACK_SYNC",
			}
			if tt.updated != "" {
				updBody["PullRequestComment"] = tt.updated
			}

			rec = doJSON(t, h, "UpdateSyncConfiguration", updBody)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			assert.Equal(t, tt.want, cfg["PullRequestComment"])
		})
	}
}

// TestCreateSyncConfiguration_Duplicate verifies that creating a sync configuration for
// the same ResourceName+SyncType twice is rejected instead of silently overwriting the
// existing configuration. Real CreateSyncConfiguration registers a dedicated
// ResourceAlreadyExistsException.
func TestCreateSyncConfiguration_Duplicate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "sync.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "", "",
	)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "develop", "other.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r2", "CFN_STACK_SYNC", "", "", "",
	)
	require.ErrorIs(t, err, codeconnections.ErrAlreadyExists)

	// The original configuration must be untouched by the rejected duplicate.
	cfg, err := b.GetSyncConfiguration(ctx, "my-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	assert.Equal(t, "main", cfg.Branch)
}
