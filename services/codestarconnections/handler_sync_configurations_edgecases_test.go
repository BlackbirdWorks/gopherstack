package codestarconnections_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSyncConfiguration_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "dup-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "dup-sync-repo")

	body := map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "dup-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	}

	rec1 := doRequest(t, h, "CreateSyncConfiguration", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateSyncConfiguration", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestCreateSyncConfiguration_ResourceNameWithSlash(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": "some-link",
		"ResourceName":     "bad/name",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteSyncConfiguration_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceName string
		syncType     string
		wantStatus   int
	}{
		{
			name:         "missing resource name",
			resourceName: "",
			syncType:     "CFN_STACK_SYNC",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "missing sync type",
			resourceName: "some-stack",
			syncType:     "",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid sync type",
			resourceName: "some-stack",
			syncType:     "INVALID",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": tt.resourceName,
				"SyncType":     tt.syncType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestListSyncConfigurations_SyncTypeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "filter-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "filter-sync-repo")

	for i := range 3 {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     "filter-stack-" + string(rune('a'+i)),
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Filter by SyncType should return all 3 (only one type supported).
	rec := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	cfgs, ok := resp["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs, 3)

	// Empty SyncType filter returns all.
	rec2 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	cfgs2, ok := resp2["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs2, 3)
}

func TestListSyncConfigurations_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "sorted-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "sorted-sync-repo")

	names := []string{"zebra-stack", "alpha-stack", "mango-stack"}
	for _, name := range names {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     name,
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	cfgs, ok := resp["SyncConfigurations"].([]any)
	require.True(t, ok)
	require.Len(t, cfgs, 3)

	cfg0 := cfgs[0].(map[string]any)
	cfg1 := cfgs[1].(map[string]any)
	cfg2 := cfgs[2].(map[string]any)
	assert.Equal(t, "alpha-stack", cfg0["ResourceName"])
	assert.Equal(t, "mango-stack", cfg1["ResourceName"])
	assert.Equal(t, "zebra-stack", cfg2["ResourceName"])
}

func TestGetSyncConfiguration_DerivedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "derived-conn", "GitLab")
	linkID := createCSCRepositoryLink(t, h, connArn, "derived-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "derived-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	cfg := resp["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "GitLab", cfg["ProviderType"])
	assert.Equal(t, "my-org", cfg["OwnerId"])
	assert.Equal(t, "derived-repo", cfg["RepositoryName"])
}

// TestSyncConfiguration_RoundTrip exercises full create/get/update/delete cycle.
func TestSyncConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "sync-conn", "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")

			// Create
			createRec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "rt-stack",
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			resp := parseResp(t, createRec)
			cfg, ok := resp["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "main", cfg["Branch"])
			assert.Equal(t, "my-org", cfg["OwnerId"])
			assert.Equal(t, "GitHub", cfg["ProviderType"])

			// Get
			getRec := doRequest(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			// Update
			updRec := doRequest(t, h, "UpdateSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
				"Branch":       "release",
			})
			require.Equal(t, http.StatusOK, updRec.Code)
			updResp := parseResp(t, updRec)
			updCfg, ok := updResp["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "release", updCfg["Branch"])

			// List
			listRec := doRequest(t, h, "ListSyncConfigurations", map[string]any{
				"RepositoryLinkId": linkID,
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResp(t, listRec)
			cfgs, ok := listResp["SyncConfigurations"].([]any)
			require.True(t, ok)
			assert.Len(t, cfgs, 1)

			// Delete
			delRec := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, delRec.Code)

			// Get after delete should fail
			afterDelRec := doRequest(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, http.StatusBadRequest, afterDelRec.Code)
		})
	}
}

// TestListRepositorySyncDefinitions exercises the handler.
func TestListRepositorySyncDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_empty",
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

			h := newTestHandler(t)
			linkID := "nonexistent"

			if tt.preCreate {
				connArn := createCSCConn(t, h, "def-conn", "GitHub")
				linkID = createCSCRepositoryLink(t, h, connArn, "my-repo")
			}

			rec := doRequest(t, h, "ListRepositorySyncDefinitions", map[string]any{
				"RepositoryLinkId": linkID,
				"SyncType":         "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				defs, ok := resp["RepositorySyncDefinitions"].([]any)
				require.True(t, ok)
				assert.NotNil(t, defs)
			}
		})
	}
}
