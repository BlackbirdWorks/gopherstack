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

func TestHandler_GetSyncBlockerSummary(t *testing.T) {
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
					"main", "config.yaml", "link-id", "blocker-resource",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "blocker-resource", "SyncType": "CFN_STACK_SYNC"},
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
		{
			name:    "missing sync type",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "some-resource"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "GetSyncBlockerSummary", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summary, ok := out["SyncBlockerSummary"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "blocker-resource", summary["ResourceName"])
			blockers, ok := summary["LatestBlockers"].([]any)
			require.True(t, ok)
			assert.Empty(t, blockers)
		})
	}
}

func TestSyncBlocker_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "blocker-lifecycle-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "blocker-lifecycle-repo")

	// Create sync config.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "blocker-lifecycle-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a blocker via backend.
	blocker, err := h.Backend.CreateSyncBlocker(
		context.Background(),
		"blocker-lifecycle-stack", "CFN_STACK_SYNC",
		"AUTOMATED", "Detected config drift",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, blocker.ID)
	assert.Equal(t, "ACTIVE", blocker.Status)

	// GetSyncBlockerSummary should show the active blocker.
	sumRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-lifecycle-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec.Code)
	sumResp := parseResp(t, sumRec)
	summary := sumResp["SyncBlockerSummary"].(map[string]any)
	blockers, ok := summary["LatestBlockers"].([]any)
	require.True(t, ok)
	require.Len(t, blockers, 1)
	blockerMap := blockers[0].(map[string]any)
	assert.Equal(t, blocker.ID, blockerMap["Id"])
	assert.Equal(t, "ACTIVE", blockerMap["Status"])
	assert.Empty(t, blockerMap["ResolvedAt"])

	// Resolve the blocker.
	updRec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"Id":             blocker.ID,
		"ResolvedReason": "Config drift corrected",
		"ResourceName":   "blocker-lifecycle-stack",
		"SyncType":       "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// GetSyncBlockerSummary should now show RESOLVED.
	sumRec2 := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-lifecycle-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec2.Code)
	sumResp2 := parseResp(t, sumRec2)
	summary2 := sumResp2["SyncBlockerSummary"].(map[string]any)
	blockers2, ok := summary2["LatestBlockers"].([]any)
	require.True(t, ok)
	require.Len(t, blockers2, 1)
	blockerMap2 := blockers2[0].(map[string]any)
	assert.Equal(t, "RESOLVED", blockerMap2["Status"])
	assert.NotEmpty(t, blockerMap2["ResolvedAt"])
	assert.Equal(t, "Config drift corrected", blockerMap2["ResolvedReason"])
}

func TestSyncBlocker_CleanedUpOnDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "blocker-cleanup-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "blocker-cleanup-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "blocker-cleanup-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	_, err := h.Backend.CreateSyncBlocker(
		context.Background(),
		"blocker-cleanup-stack", "CFN_STACK_SYNC",
		"MANUAL", "Manual block",
	)
	require.NoError(t, err)

	// Delete the sync configuration.
	delRec := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "blocker-cleanup-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// GetSyncBlockerSummary should now fail (config deleted).
	getRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-cleanup-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)
}

func TestUpdateSyncBlocker_UnknownID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"Id":             "unknown-blocker-id",
		"ResolvedReason": "just in case",
		"ResourceName":   "some-stack",
		"SyncType":       "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	resp := parseResp(t, rec)
	assert.Equal(t, "SyncBlockerDoesNotExistException", resp["__type"])
}

func TestUpdateSyncBlocker_MissingId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"ResolvedReason": "no id here",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestUpdateSyncBlocker_RequiredFields verifies ResourceName, SyncType, and
// ResolvedReason are enforced as required input, matching real
// UpdateSyncBlockerInput (all four members -- Id, ResolvedReason,
// ResourceName, SyncType -- are "This member is required" in
// aws-sdk-go-v2's generated api_op_UpdateSyncBlocker.go).
func TestUpdateSyncBlocker_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing_resolved_reason",
			body: map[string]any{"Id": "some-id", "ResourceName": "my-stack", "SyncType": "CFN_STACK_SYNC"},
		},
		{
			name: "missing_resource_name",
			body: map[string]any{"Id": "some-id", "ResolvedReason": "fixed", "SyncType": "CFN_STACK_SYNC"},
		},
		{
			name: "missing_sync_type",
			body: map[string]any{"Id": "some-id", "ResolvedReason": "fixed", "ResourceName": "my-stack"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateSyncBlocker", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func TestGetSyncBlockerSummary_EmptyBlockersIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "empty-blocker-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "empty-blocker-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "empty-blocker-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	sumRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "empty-blocker-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec.Code)

	resp := parseResp(t, sumRec)
	summary := resp["SyncBlockerSummary"].(map[string]any)
	blockers, ok := summary["LatestBlockers"].([]any)
	require.True(t, ok, "LatestBlockers should be an array, not null")
	assert.Empty(t, blockers)
}

// TestGetSyncBlockerSummary exercises the handler.
func TestGetSyncBlockerSummary(t *testing.T) {
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

			h := newTestHandler(t)

			if tt.preCreate {
				connArn := createCSCConn(t, h, "blocker-conn", "GitHub")
				linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")
				rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "blocker-stack",
					"RoleArn":          "arn:aws:iam::000000000000:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
				"ResourceName": "blocker-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				summary, ok := resp["SyncBlockerSummary"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "blocker-stack", summary["ResourceName"])
			}
		})
	}
}

// TestUpdateSyncBlocker_RequiresId verifies Id is required, and that a
// non-empty but unknown Id fails differently (SyncBlockerDoesNotExistException
// from the backend) than an empty Id (InvalidInputException from input
// validation, before the backend is ever consulted).
func TestUpdateSyncBlocker_RequiresId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		id          string
		wantErrType string
	}{
		{
			name:        "with_id_but_unknown",
			id:          "blocker-abc",
			wantErrType: "SyncBlockerDoesNotExistException",
		},
		{
			name:        "missing_id",
			id:          "",
			wantErrType: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ResolvedReason": "fixed",
				"ResourceName":   "my-stack",
				"SyncType":       "CFN_STACK_SYNC",
			}
			if tt.id != "" {
				body["Id"] = tt.id
			}

			rec := doRequest(t, h, "UpdateSyncBlocker", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErrType, resp["__type"])
		})
	}
}
