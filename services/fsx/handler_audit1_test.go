package fsx_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func newTestHandler(t *testing.T) *fsx.Handler {
	t.Helper()
	backend := fsx.NewInMemoryBackend("000000000000", "us-east-1")

	return fsx.NewHandler(backend)
}

func doFSxRequest(t *testing.T, h *fsx.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSSimbaAPIService_v20180301."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createFS(t *testing.T, h *fsx.Handler, fsType string) string {
	t.Helper()
	rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": fsType})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["FileSystem"].(map[string]any)["FileSystemId"].(string)
}

func createFSandBackup(t *testing.T, h *fsx.Handler, fsType string) string {
	t.Helper()
	fsID := createFS(t, h, fsType)
	rec := doFSxRequest(t, h, "CreateBackup", map[string]any{"FileSystemId": fsID})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["Backup"].(map[string]any)["BackupId"].(string)
}

func TestFSx_FileSystem(t *testing.T) {
	t.Parallel()

	t.Run("CreateFileSystem returns id and arn", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(
			t,
			h,
			"CreateFileSystem",
			map[string]any{"FileSystemType": "LUSTRE", "StorageCapacity": 1200},
		)
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fs := resp["FileSystem"].(map[string]any)
		assert.Contains(t, fs["FileSystemId"].(string), "fs-")
		assert.Contains(t, fs["ResourceARN"].(string), "arn:aws:fsx:")
		assert.Equal(t, "AVAILABLE", fs["Lifecycle"])
		assert.Equal(t, "LUSTRE", fs["FileSystemType"])
	})

	t.Run("CreateFileSystem missing type returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DescribeFileSystems returns all file systems", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createFS(t, h, "WINDOWS")
		createFS(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fss := resp["FileSystems"].([]any)
		assert.Len(t, fss, 2)
	})

	t.Run("DescribeFileSystems by id returns specific file system", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "OPENZFS")
		createFS(t, h, "WINDOWS")
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{"FileSystemIds": []string{id}})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fss := resp["FileSystems"].([]any)
		require.Len(t, fss, 1)
		assert.Equal(t, id, fss[0].(map[string]any)["FileSystemId"])
	})

	t.Run("DescribeFileSystems unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "DescribeFileSystems", map[string]any{"FileSystemIds": []string{"fs-notexist"}})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DeleteFileSystem returns 200", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "LUSTRE")
		rec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": id})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, 0, fsx.FileSystemCount(fsx.GetBackend(h)))
	})

	t.Run("DeleteFileSystem unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "DeleteFileSystem", map[string]any{"FileSystemId": "fs-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("UpdateFileSystem returns updated FileSystem", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		id := createFS(t, h, "WINDOWS")
		rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{"FileSystemId": id, "StorageCapacity": 64})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		fs := resp["FileSystem"].(map[string]any)
		assert.Equal(t, id, fs["FileSystemId"])
	})

	t.Run("UpdateFileSystem unknown id returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "UpdateFileSystem", map[string]any{"FileSystemId": "fs-notexist"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

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

func TestFSx_Tags(t *testing.T) {
	t.Parallel()

	fsARN := func(t *testing.T, h *fsx.Handler) string {
		t.Helper()
		rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{"FileSystemType": "LUSTRE"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		return out["FileSystem"].(map[string]any)["ResourceARN"].(string)
	}

	t.Run("TagResource adds tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		rec := doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListTagsForResource returns tags", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "Name", "Value": "myfs"}},
		})
		rec := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		tags := resp["Tags"].([]any)
		assert.Len(t, tags, 1)
		tag := tags[0].(map[string]any)
		assert.Equal(t, "Name", tag["Key"])
		assert.Equal(t, "myfs", tag["Value"])
	})

	t.Run("UntagResource removes tag", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		arn := fsARN(t, h)
		doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": arn,
			"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}, {"Key": "Name", "Value": "x"}},
		})
		doFSxRequest(t, h, "UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"env"}})
		rec := doFSxRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		tags := resp["Tags"].([]any)
		assert.Len(t, tags, 1)
		assert.Equal(t, "Name", tags[0].(map[string]any)["Key"])
	})

	t.Run("TagResource unknown arn returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doFSxRequest(t, h, "TagResource", map[string]any{
			"ResourceARN": "arn:aws:fsx:us-east-1:000000000000:file-system/fs-notexist",
			"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestFSx_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doFSxRequest(t, h, "CreateVolume", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
