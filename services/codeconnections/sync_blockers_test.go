package codeconnections_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestGetSyncBlockerSummary exercises the GetSyncBlockerSummary handler.
func TestGetSyncBlockerSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_returns_summary",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource_name",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "blocker-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "blocker-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"SyncType": "CFN_STACK_SYNC",
			}
			if tt.name != "missing_resource_name" {
				body["ResourceName"] = "blocker-stack"
			}

			rec := doJSON(t, h, "GetSyncBlockerSummary", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				summary, ok := resp["SyncBlockerSummary"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "blocker-stack", summary["ResourceName"])
				_, hasBlockers := summary["LatestBlockers"]
				assert.True(t, hasBlockers)
			}
		})
	}
}

// TestUpdateSyncBlocker verifies UpdateSyncBlocker validates Id is required,
// resolves a real pre-existing blocker under the singular "SyncBlocker" wire key
// (not "SyncBlockerSummary"), and rejects an unknown ID with
// SyncBlockerDoesNotExistException -- the real operation documents this
// exception and does not resolve unknown IDs gracefully.
func TestUpdateSyncBlocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		preCreate  bool
		wantStatus int
	}{
		{
			name:       "success_with_real_blocker",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown_id_not_found",
			id:         "some-blocker-id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			id:         "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "sb-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "my-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			id := tt.id
			if tt.preCreate {
				blocker, err := h.Backend.CreateSyncBlocker(
					context.Background(), "my-stack", "CFN_STACK_SYNC", "AUTOMATED", "blocked",
				)
				require.NoError(t, err)
				id = blocker.ID
			}

			body := map[string]any{
				"ResolvedReason": "issue fixed",
				"ResourceName":   "my-stack",
				"SyncType":       "CFN_STACK_SYNC",
			}
			if id != "" {
				body["Id"] = id
			}

			rec = doJSON(t, h, "UpdateSyncBlocker", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				blockerResp, ok := resp["SyncBlocker"].(map[string]any)
				require.True(t, ok, "real UpdateSyncBlockerOutput wraps the resolved blocker "+
					"under the singular 'SyncBlocker' key, not 'SyncBlockerSummary'")
				assert.Equal(t, id, blockerResp["Id"])
				assert.Equal(t, "RESOLVED", blockerResp["Status"])
				assert.Equal(t, "issue fixed", blockerResp["ResolvedReason"])
				assert.Equal(t, "my-stack", resp["ResourceName"])
				_, hasSummary := resp["SyncBlockerSummary"]
				assert.False(t, hasSummary, "response must not contain the fabricated SyncBlockerSummary key")
			}
		})
	}
}

// TestUpdateSyncBlockerResourceName verifies that UpdateSyncBlocker returns the
// real blocker's ResourceName (from the resolved blocker record, not merely echoed from
// the request) in the response, and that an unknown blocker ID is rejected rather than
// resolved gracefully -- the real operation documents SyncBlockerDoesNotExistException
// for exactly this case.
func TestUpdateSyncBlockerResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantName   string
		wantStatus int
		useRealID  bool
	}{
		{name: "resource_name_echoed", useRealID: true, wantStatus: http.StatusOK, wantName: "my-cfn-stack"},
		{name: "unknown_id_rejected", useRealID: false, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "sb-name-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "my-cfn-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			id := "blocker-123"
			if tt.useRealID {
				blocker, err := h.Backend.CreateSyncBlocker(
					context.Background(), "my-cfn-stack", "CFN_STACK_SYNC", "AUTOMATED", "blocked",
				)
				require.NoError(t, err)
				id = blocker.ID
			}

			body := map[string]any{
				"Id":             id,
				"SyncType":       "CFN_STACK_SYNC",
				"ResourceName":   "my-cfn-stack",
				"ResolvedReason": "fixed",
			}

			rec = doJSON(t, h, "UpdateSyncBlocker", body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantName, resp["ResourceName"])
			}
		})
	}
}

// TestSyncBlocker_CreateGetResolve exercises the real sync blocker lifecycle: creating a
// blocker, reading it back via GetSyncBlockerSummary, and resolving it via
// UpdateSyncBlocker.
func TestSyncBlocker_CreateGetResolve(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	conn, err := b.CreateConnection(ctx, "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	link, err := b.CreateRepositoryLink(ctx, conn.ConnectionArn, "my-org", "my-repo", "", nil)
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		ctx, "main", "sync.yaml", link.RepositoryLinkID, "my-stack",
		"arn:aws:iam::123456789012:role/r", "CFN_STACK_SYNC", "", "",
	)
	require.NoError(t, err)

	blocker, err := b.CreateSyncBlocker(ctx, "my-stack", "CFN_STACK_SYNC", "AUTOMATED", "drift detected")
	require.NoError(t, err)
	assert.Equal(t, codeconnections.SyncBlockerStatusActive, blocker.Status)

	summary, err := b.GetSyncBlockerSummary(ctx, "my-stack", "CFN_STACK_SYNC")
	require.NoError(t, err)
	require.Len(t, summary.LatestBlockers, 1)
	assert.Equal(t, blocker.ID, summary.LatestBlockers[0].ID)
	assert.Equal(t, codeconnections.SyncBlockerStatusActive, summary.LatestBlockers[0].Status)

	resolved, err := b.UpdateSyncBlocker(ctx, blocker.ID, "fixed")
	require.NoError(t, err)
	require.Len(t, resolved.LatestBlockers, 1)
	assert.Equal(t, codeconnections.SyncBlockerStatusResolved, resolved.LatestBlockers[0].Status)
	assert.Equal(t, "fixed", resolved.LatestBlockers[0].ResolvedReason)

	_, err = b.UpdateSyncBlocker(ctx, "nonexistent-id", "fixed")
	require.Error(t, err)
	assert.ErrorIs(t, err, codeconnections.ErrSyncBlockerNotFound)
}
