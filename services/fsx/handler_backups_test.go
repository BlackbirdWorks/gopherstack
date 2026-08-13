package fsx_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_Backup(t *testing.T) {
	t.Parallel()

	t.Run("CreateBackup returns Backup with id and arn", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		bk := resp["Backup"].(map[string]any)
		assert.Contains(t, bk["BackupId"].(string), "backup-")
		assert.Contains(t, bk["ResourceARN"].(string), "arn:aws:fsx:")
		assert.Equal(t, "AVAILABLE", bk["Lifecycle"])
		assert.Equal(t, "USER_INITIATED", bk["Type"])
	})

	t.Run("CreateBackup unknown fs returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": "fs-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DescribeBackups returns all backups", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "WINDOWS")
		doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
		doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
		rec := doFSxRequest(t, h, "DescribeBackups", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		bks := resp["Backups"].([]any)
		assert.Len(t, bks, 2)
	})

	t.Run("DescribeBackups by id returns specific backup", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		bkID := createFSandBackup(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DescribeBackups", map[string]any{"BackupIds": []string{bkID}})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		bks := resp["Backups"].([]any)
		require.Len(t, bks, 1)
		assert.Equal(t, bkID, bks[0].(map[string]any)["BackupId"])
	})

	t.Run("DeleteBackup returns 200", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		bkID := createFSandBackup(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DeleteBackup", map[string]any{"BackupId": bkID})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, 0, fsx.BackupCount(fsx.GetBackend(h)))
	})

	t.Run("DeleteBackup unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "DeleteBackup", map[string]any{"BackupId": "backup-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("CreateFileSystemFromBackup returns new FileSystem", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		bkID := createFSandBackup(t, h, "WINDOWS")
		rec := doFSxRequest(t, h, "CreateFileSystemFromBackup", map[string]any{"BackupId": bkID})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fs := resp["FileSystem"].(map[string]any)
		assert.Contains(t, fs["FileSystemId"].(string), "fs-")
		assert.Equal(t, "AVAILABLE", fs["Lifecycle"])
		assert.Equal(t, 2, fsx.FileSystemCount(fsx.GetBackend(h)))
	})

	t.Run("CreateFileSystemFromBackup unknown backup returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "CreateFileSystemFromBackup", map[string]any{"BackupId": "backup-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestFSx_CreateFileSystemFromBackup_FileSystemTypeVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request map[string]any
		want    string
	}{
		{
			name:    "explicit version overrides the backup default",
			request: map[string]any{"FileSystemTypeVersion": "2.15"},
			want:    "2.15",
		},
		{
			name:    "omitted version leaves the field empty",
			request: map[string]any{},
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bkID := createFSandBackup(t, h, "LUSTRE")

			body := map[string]any{"BackupId": bkID}
			maps.Copy(body, tt.request)

			rec := doFSxRequest(t, h, "CreateFileSystemFromBackup", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			fs := resp["FileSystem"].(map[string]any)

			got, _ := fs["FileSystemTypeVersion"].(string)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFSx_CopyBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *fsx.Handler) string
		name     string
		wantErr  string
		wantCode int
	}{
		{
			name: "copies backup and returns new BackupId",
			setup: func(h *fsx.Handler) string {
				return createFSandBackup(t, h, "WINDOWS")
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown source backup returns 400",
			setup:    func(_ *fsx.Handler) string { return "backup-does-not-exist" },
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			srcID := tc.setup(h)

			rec := doFSxRequest(t, h, "CopyBackup", map[string]any{"SourceBackupId": srcID})
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				bk := out["Backup"].(map[string]any)
				assert.Contains(t, bk["BackupId"].(string), "backup-")
				newID := bk["BackupId"].(string)
				assert.NotEqual(t, srcID, newID)
			}
		})
	}
}

func TestFSx_DeleteBackup_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantLC   string
		wantCode int
	}{
		{
			name:     "returns BackupId and DELETED lifecycle",
			wantCode: http.StatusOK,
			wantLC:   "DELETED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			bkID := createFSandBackup(t, h, "WINDOWS")

			rec := doFSxRequest(t, h, "DeleteBackup", map[string]any{"BackupId": bkID})
			require.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, bkID, resp["BackupId"])
			assert.Equal(t, tc.wantLC, resp["Lifecycle"])
		})
	}
}

func TestFSx_BackupNotFound_ErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
	}{
		{
			name:     "DeleteBackup unknown id returns BackupNotFound code",
			op:       "DeleteBackup",
			body:     map[string]any{"BackupId": "backup-notexist"},
			wantCode: http.StatusBadRequest,
			wantType: "BackupNotFound",
		},
		{
			name:     "DescribeBackups unknown id returns BackupNotFound code",
			op:       "DescribeBackups",
			body:     map[string]any{"BackupIds": []string{"backup-notexist"}},
			wantCode: http.StatusBadRequest,
			wantType: "BackupNotFound",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doFSxRequest(t, h, tc.op, tc.body)
			require.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp["__type"])
		})
	}
}
