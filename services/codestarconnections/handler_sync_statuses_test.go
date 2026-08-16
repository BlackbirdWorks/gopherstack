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

func TestHandler_GetRepositorySyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				link, err := h.Backend.CreateRepositoryLink(
					context.Background(),
					"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
					"owner", "repo", "", nil,
				)
				if err != nil {
					return ""
				}

				return link.RepositoryLinkID
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "nonexistent-id"
			},
			wantErr: true,
		},
		{
			name: "missing link id",
			setupFn: func(_ *codestarconnections.Handler) string {
				return ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupFn(h)

			rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": id,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			latest, ok := out["LatestSync"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", latest["Status"])
			assert.NotEmpty(t, latest["StartedAt"])
		})
	}
}

func TestHandler_GetResourceSyncStatus(t *testing.T) {
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
					"main", "config.yaml", "link-id", "my-resource",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "my-resource", "SyncType": "CFN_STACK_SYNC"},
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

			rec := doRequest(t, h, "GetResourceSyncStatus", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			latest, ok := out["LatestSync"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", latest["Status"])
			assert.NotEmpty(t, latest["StartedAt"])
		})
	}
}

func TestGetRepositorySyncStatus_RealState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setStatus  string
		wantStatus string
	}{
		{name: "SUCCEEDED", setStatus: "SUCCEEDED", wantStatus: "SUCCEEDED"},
		{name: "FAILED", setStatus: "FAILED", wantStatus: "FAILED"},
		{name: "IN_PROGRESS", setStatus: "IN_PROGRESS", wantStatus: "IN_PROGRESS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "rsync-"+tt.name, "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "rsync-repo")

			// Set a specific sync status via backend helper.
			h.Backend.SetRepositorySyncStatus(
				context.Background(),
				linkID, "main", "CFN_STACK_SYNC",
				tt.setStatus, nil,
			)

			rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			latest := resp["LatestSync"].(map[string]any)
			assert.Equal(t, tt.wantStatus, latest["Status"])
		})
	}
}

func TestGetResourceSyncStatus_RealState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setStatus  string
		wantStatus string
	}{
		{name: "SUCCEEDED", setStatus: "SUCCEEDED", wantStatus: "SUCCEEDED"},
		{name: "FAILED", setStatus: "FAILED", wantStatus: "FAILED"},
		{name: "IN_PROGRESS", setStatus: "IN_PROGRESS", wantStatus: "IN_PROGRESS"},
		{name: "QUEUED", setStatus: "QUEUED", wantStatus: "QUEUED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "rss-conn-"+tt.name, "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "rss-repo-"+tt.name)

			rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "cfg.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "rss-stack-" + tt.name,
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			h.Backend.SetResourceSyncStatus(
				context.Background(),
				"rss-stack-"+tt.name, "CFN_STACK_SYNC",
				tt.setStatus, nil,
			)

			getRec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "rss-stack-" + tt.name,
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			resp := parseResp(t, getRec)
			latest := resp["LatestSync"].(map[string]any)
			assert.Equal(t, tt.wantStatus, latest["Status"])
		})
	}
}

func TestGetResourceSyncStatus_AutoSeeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "auto-seed-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "auto-seed-repo")

	// Create a sync configuration.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "auto-seed-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Immediately get sync status - should be seeded with SUCCEEDED.
	getRec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "auto-seed-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	resp := parseResp(t, getRec)
	latest := resp["LatestSync"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", latest["Status"])
}

// TestGetResourceSyncStatus_IncludesTarget verifies LatestSync.Target is
// populated with the resource name. Target is a required real member of
// types.ResourceSyncAttempt (confirmed against aws-sdk-go-v2's
// awsAwsjson10_deserializeDocumentResourceSyncAttempt) that a previous
// implementation omitted entirely.
func TestGetResourceSyncStatus_IncludesTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "target-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "target-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "target-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "target-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseResp(t, getRec)
	latest := resp["LatestSync"].(map[string]any)
	assert.Equal(t, "target-stack", latest["Target"])
}

func TestGetRepositorySyncStatus_WithEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "sync-events-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "sync-events-repo")

	events := []codestarconnections.SyncEvent{
		{Event: "start", Type: "info"},
		{Event: "apply", Type: "info", ExternalID: "ext-123"},
	}
	h.Backend.SetRepositorySyncStatus(
		context.Background(),
		linkID, "main", "CFN_STACK_SYNC",
		"SUCCEEDED", events,
	)

	rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	latest := resp["LatestSync"].(map[string]any)
	evts, ok := latest["Events"].([]any)
	require.True(t, ok)
	require.Len(t, evts, 2)

	evt0 := evts[0].(map[string]any)
	assert.Equal(t, "start", evt0["Event"])
	assert.Equal(t, "info", evt0["Type"])
	assert.Empty(t, evt0["ExternalId"])

	evt1 := evts[1].(map[string]any)
	assert.Equal(t, "apply", evt1["Event"])
	assert.Equal(t, "ext-123", evt1["ExternalId"])
}

func TestGetRepositorySyncStatus_StartedAtFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "ts-format-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "ts-format-repo")

	rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	latest := resp["LatestSync"].(map[string]any)
	startedAt, ok := latest["StartedAt"].(float64)
	require.True(t, ok, "StartedAt must decode as a JSON number (epoch seconds)")
	assert.Positive(t, startedAt)
}

// TestGetRepositorySyncStatus exercises the handler with valid and invalid inputs.
func TestGetRepositorySyncStatus(t *testing.T) {
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
			linkID := "nonexistent-link"

			if tt.preCreate {
				connArn := createCSCConn(t, h, "sync-status-conn", "GitHub")
				linkID = createCSCRepositoryLink(t, h, connArn, "my-repo")
			}

			rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestGetResourceSyncStatus exercises the handler.
func TestGetResourceSyncStatus(t *testing.T) {
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
				connArn := createCSCConn(t, h, "res-sync-conn", "GitHub")
				linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")
				rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "res-sync-stack",
					"RoleArn":          "arn:aws:iam::000000000000:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "res-sync-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
			}
		})
	}
}
