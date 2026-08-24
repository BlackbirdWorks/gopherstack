package codestarconnections_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

func TestHandler_CreateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "config.yaml",
				"RepositoryLinkId": "link-id",
				"ResourceName":     "my-stack",
				"RoleArn":          "arn:aws:iam::000000000000:role/my-role",
				"SyncType":         "CFN_STACK_SYNC",
			},
			wantErr: false,
		},
		{
			name: "missing branch",
			body: map[string]any{
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
				"SyncType":         "CFN_STACK_SYNC",
			},
			wantErr: true,
		},
		{
			name: "invalid sync type",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
				"SyncType":         "INVALID",
			},
			wantErr: true,
		},
		{
			name: "missing sync type",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateSyncConfiguration", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			cfg, ok := out["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "main", cfg["Branch"])
			assert.Equal(t, "my-stack", cfg["ResourceName"])
			assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
		})
	}
}

func TestHandler_GetSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "my-stack",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "my-stack", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
		{
			name:    "missing resource name",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "GetSyncConfiguration", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			cfg, ok := out["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "my-stack", cfg["ResourceName"])
			assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
		})
	}
}

func TestHandler_DeleteSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "del-stack",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "del-stack", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			// DeleteSyncConfiguration is idempotent in real AWS: its own
			// error switch has no ResourceNotFoundException case, unlike
			// GetSyncConfiguration.
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "DeleteSyncConfiguration", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_RepositoryLink_SyncConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	conn, err := h.Backend.CreateConnection(context.Background(), "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// Create a repository link.
	recLink := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  conn.ConnectionArn,
		"OwnerId":        "my-owner",
		"RepositoryName": "my-repo",
	})
	require.Equal(t, http.StatusOK, recLink.Code)

	var linkOut map[string]any
	require.NoError(t, json.Unmarshal(recLink.Body.Bytes(), &linkOut))
	linkInfo := linkOut["RepositoryLinkInfo"].(map[string]any)
	linkID := linkInfo["RepositoryLinkId"].(string)
	require.NotEmpty(t, linkID)

	// Create a sync configuration using the link.
	recSync := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "my-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/sync-role",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recSync.Code)

	var syncOut map[string]any
	require.NoError(t, json.Unmarshal(recSync.Body.Bytes(), &syncOut))
	syncCfg := syncOut["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "my-repo", syncCfg["RepositoryName"])
	assert.Equal(t, "GitHub", syncCfg["ProviderType"])

	// Get repository sync status.
	recStatus := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recStatus.Code)

	// Get resource sync status.
	recResStatus := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recResStatus.Code)

	// List repository links.
	recList := doRequest(t, h, "ListRepositoryLinks", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	links, ok := listOut["RepositoryLinks"].([]any)
	require.True(t, ok)
	assert.Len(t, links, 1)

	// Delete sync configuration.
	recDelSync := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, recDelSync.Code)

	// Delete repository link.
	recDelLink := doRequest(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
	assert.Equal(t, http.StatusOK, recDelLink.Code)
}

// TestSyncTypeValidation verifies invalid SyncType is rejected.
func TestSyncTypeValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": "link-id",
		"ResourceName":     "stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "INVALID_SYNC_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListSyncConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "pag-syncs-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "pag-syncs-repo")

	for i := range 5 {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     "stack-" + string(rune('a'+i)),
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"MaxResults":       3,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	cfgs1, ok := resp1["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs1, 3)

	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "")

	rec2 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"MaxResults":       3,
		"NextToken":        nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	cfgs2, ok := resp2["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs2, 2)
	assert.Empty(t, resp2["NextToken"])
}

func TestSyncConfiguration_PublishAndTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		publishDeploymentStatus string
		triggerResourceUpdateOn string
		wantPublish             string
		wantTrigger             string
		wantStatus              int
	}{
		{
			name:                    "ENABLED publish and ANY_CHANGE trigger",
			publishDeploymentStatus: "ENABLED",
			triggerResourceUpdateOn: "ANY_CHANGE",
			wantStatus:              http.StatusOK,
			wantPublish:             "ENABLED",
			wantTrigger:             "ANY_CHANGE",
		},
		{
			name:                    "DISABLED publish and FILE_CHANGE trigger",
			publishDeploymentStatus: "DISABLED",
			triggerResourceUpdateOn: "FILE_CHANGE",
			wantStatus:              http.StatusOK,
			wantPublish:             "DISABLED",
			wantTrigger:             "FILE_CHANGE",
		},
		{
			name:                    "invalid publish status",
			publishDeploymentStatus: "INVALID",
			triggerResourceUpdateOn: "ANY_CHANGE",
			wantStatus:              http.StatusBadRequest,
		},
		{
			name:                    "invalid trigger value",
			publishDeploymentStatus: "ENABLED",
			triggerResourceUpdateOn: "NEVER",
			wantStatus:              http.StatusBadRequest,
		},
		{
			name:       "no publish or trigger (omitted)",
			wantStatus: http.StatusOK,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "pt-conn-"+string(rune('a'+i)), "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "pt-repo-"+string(rune('a'+i)))

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "cfg.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "pt-stack-" + string(rune('a'+i)),
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}

			if tt.publishDeploymentStatus != "" {
				body["PublishDeploymentStatus"] = tt.publishDeploymentStatus
			}

			if tt.triggerResourceUpdateOn != "" {
				body["TriggerResourceUpdateOn"] = tt.triggerResourceUpdateOn
			}

			rec := doRequest(t, h, "CreateSyncConfiguration", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantPublish != "" {
				resp := parseResp(t, rec)
				cfg := resp["SyncConfiguration"].(map[string]any)
				assert.Equal(t, tt.wantPublish, cfg["PublishDeploymentStatus"])
				assert.Equal(t, tt.wantTrigger, cfg["TriggerResourceUpdateOn"])
			}
		})
	}
}

func TestUpdateSyncConfiguration_PublishAndTrigger(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "upd-pt-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "upd-pt-repo")

	// Create with ENABLED/ANY_CHANGE.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":                  "main",
		"ConfigFile":              "cfg.yaml",
		"RepositoryLinkId":        linkID,
		"ResourceName":            "upd-pt-stack",
		"RoleArn":                 "arn:aws:iam::000000000000:role/r",
		"SyncType":                "CFN_STACK_SYNC",
		"PublishDeploymentStatus": "ENABLED",
		"TriggerResourceUpdateOn": "ANY_CHANGE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update to DISABLED/FILE_CHANGE.
	updRec := doRequest(t, h, "UpdateSyncConfiguration", map[string]any{
		"ResourceName":            "upd-pt-stack",
		"SyncType":                "CFN_STACK_SYNC",
		"PublishDeploymentStatus": "DISABLED",
		"TriggerResourceUpdateOn": "FILE_CHANGE",
	})
	require.Equal(t, http.StatusOK, updRec.Code)
	updResp := parseResp(t, updRec)
	cfg := updResp["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "DISABLED", cfg["PublishDeploymentStatus"])
	assert.Equal(t, "FILE_CHANGE", cfg["TriggerResourceUpdateOn"])
}
