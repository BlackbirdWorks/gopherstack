package codeconnections_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestGetRepositorySyncStatus exercises the GetRepositorySyncStatus handler.
func TestGetRepositorySyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
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
			wantStatus: http.StatusOK,
			wantSync:   true,
		},
		{
			name: "not_found",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "nonexistent-id"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_repository_link_id",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestGetResourceSyncStatus exercises the GetResourceSyncStatus handler.
func TestGetResourceSyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantSync   bool
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
			wantSync:   true,
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

			rec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "my-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestListRepositorySyncDefinitionsHandler exercises the ListRepositorySyncDefinitions handler.
func TestListRepositorySyncDefinitionsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_returns_empty",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repository_link_id",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := "nonexistent-link"

			if tt.preCreate {
				connArn := createConn(t, h, "def-conn", "GitHub")
				linkID = createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			}

			body := map[string]any{
				"SyncType": "CFN_STACK_SYNC",
			}
			if tt.name != "missing_repository_link_id" {
				body["RepositoryLinkId"] = linkID
			}

			rec := doJSON(t, h, "ListRepositorySyncDefinitions", body)
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

// TestListRepositorySyncDefinitions_DerivedFromSyncConfig verifies real sync definitions
// are derived from the repository link's sync configurations (Branch/ConfigFile-as-
// Directory/ResourceName-as-Target+Parent), not a hardcoded empty stub.
func TestListRepositorySyncDefinitions_DerivedFromSyncConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "template.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
	)
	require.NoError(t, err)

	defs, err := b.ListRepositorySyncDefinitions(ctx, link.RepositoryLinkID, "CFN_STACK_SYNC")
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "main", defs[0].Branch)
	assert.Equal(t, "template.yaml", defs[0].Directory)
	// Real AWS docs: "for CFN_STACK_SYNC the parent and target resource are the same".
	assert.Equal(t, "my-stack", defs[0].Parent)
	assert.Equal(t, "my-stack", defs[0].Target)
}
