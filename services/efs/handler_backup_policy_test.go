package efs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestDescribeBackupPolicy exercises DescribeBackupPolicy.
func TestDescribeBackupPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "returns_disabled_by_default",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "backup-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodGet,
					"/2015-02-01/file-systems/"+fsID+"/backup-policy", nil)
				require.Equal(t, http.StatusOK, rec2.Code)

				resp := parseResp(t, rec2)
				bp, ok := resp["BackupPolicy"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "DISABLED", bp["Status"])
			},
		},
		{
			name: "missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodGet,
					"/2015-02-01/file-systems/fs-notexist/backup-policy", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestEFSHandler()
			tt.ops(t, h)
		})
	}
}

// TestPutBackupPolicy verifies PutBackupPolicy sets the backup policy status.
func TestPutBackupPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "enable_backup", status: "ENABLED"},
		{name: "disable_backup", status: "DISABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-bp-"+tt.name)

			rec := doREST(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/backup-policy",
				map[string]any{
					"BackupPolicy": map[string]any{"Status": tt.status},
				})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify via describe.
			rec2 := doREST(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/backup-policy", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			resp2 := parseResp(t, rec2)
			bp := resp2["BackupPolicy"].(map[string]any)
			assert.Equal(t, tt.status, bp["Status"])
		})
	}
}

// TestBackupPolicy_EnabledRoundTrip verifies PutBackupPolicy stores the status
// and DescribeBackupPolicy returns it. Real AWS stores and returns the backup policy status.
func TestBackupPolicy_EnabledRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "backup-roundtrip",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	putRec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/backup-policy",
		map[string]any{"BackupPolicy": map[string]any{"Status": "ENABLED"}})
	require.Equal(t, http.StatusOK, putRec.Code, "PutBackupPolicy failed: %s", putRec.Body.String())

	descRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/backup-policy", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		BackupPolicy struct {
			Status string `json:"Status"`
		} `json:"BackupPolicy"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, "ENABLED", descOut.BackupPolicy.Status)
}

// TestBackupPolicy_InvalidStatusRejected verifies that PutBackupPolicy returns 400
// for an unrecognized status value, matching real AWS ValidationException.
func TestBackupPolicy_InvalidStatusRejected(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "backup-invalid",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/backup-policy",
		map[string]any{"BackupPolicy": map[string]any{"Status": "BOGUS_STATUS"}})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"PutBackupPolicy with invalid status must return 400; body: %s", rec.Body.String())
}
