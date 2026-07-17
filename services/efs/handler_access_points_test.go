package efs_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestAccessPointCRUD exercises CreateAccessPoint, DescribeAccessPoints and DeleteAccessPoint.
func TestAccessPointCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "create_describe_delete",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				// Create file system first.
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "ap-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// Create access point.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": fsID,
					"Tags":         []map[string]string{{"Key": "Name", "Value": "my-ap"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				ap := parseResp(t, rec2)
				assert.Equal(t, fsID, ap["FileSystemId"])
				apID := ap["AccessPointId"].(string)
				assert.NotEmpty(t, apID)

				// Describe by FS.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/access-points", nil)
				assert.Equal(t, http.StatusOK, rec3.Code)
				list := parseResp(t, rec3)["AccessPoints"].([]any)
				assert.Len(t, list, 1)

				// Delete.
				rec4 := doREST(t, h, http.MethodDelete, "/2015-02-01/access-points/"+apID, nil)
				assert.Equal(t, http.StatusNoContent, rec4.Code)

				// Describe after delete returns empty.
				rec5 := doREST(t, h, http.MethodGet, "/2015-02-01/access-points", nil)
				assert.Equal(t, http.StatusOK, rec5.Code)
				list2 := parseResp(t, rec5)["AccessPoints"].([]any)
				assert.Empty(t, list2)
			},
		},
		{
			name: "create_access_point_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": "fs-notexist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_access_point_missing_fs_id_returns_400",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": "",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_non_existent_access_point_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/2015-02-01/access-points/fsap-notexist", nil)
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

// TestDescribeAccessPointByID tests describing a specific access point by ID.
func TestDescribeAccessPointByID(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "ap-id-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create access point.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	apID := parseResp(t, rec2)["AccessPointId"].(string)

	// Describe by ID via path.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/access-points/"+apID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	list := parseResp(t, rec3)["AccessPoints"].([]any)
	assert.Len(t, list, 1)
}

// TestSortedDescribeAccessPoints verifies sorted access points.
func TestSortedDescribeAccessPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "multiple_sorted", count: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-ap-sort-"+tt.name)

			for range tt.count {
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/access-points",
					map[string]any{
						"FileSystemId": fsID,
					},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doREST(t, h, http.MethodGet, "/2015-02-01/access-points", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			list, ok := resp["AccessPoints"].([]any)
			require.True(t, ok)
			require.Len(t, list, tt.count)

			for i := 1; i < len(list); i++ {
				prev := list[i-1].(map[string]any)["AccessPointId"].(string)
				curr := list[i].(map[string]any)["AccessPointId"].(string)
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

// TestAccessPointResponse verifies ClientToken and PosixUser appear in HTTP response.
func TestAccessPointResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantClientToken bool
		wantPosixUser   bool
		wantRootDir     bool
	}{
		{
			name: "client_token_in_response",
			body: map[string]any{
				"FileSystemId": "",
				"ClientToken":  "ct-resp-test",
			},
			wantClientToken: true,
		},
		{
			name: "posix_user_in_response",
			body: map[string]any{
				"FileSystemId": "",
				"PosixUser": map[string]any{
					"Uid": 1000,
					"Gid": 1001,
				},
			},
			wantPosixUser: true,
		},
		{
			name: "root_directory_in_response",
			body: map[string]any{
				"FileSystemId": "",
				"RootDirectory": map[string]any{
					"Path": "/",
				},
			},
			wantRootDir: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-ap-resp-"+tt.name)
			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["FileSystemId"] = fsID

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)

			if tt.wantClientToken {
				assert.NotEmpty(t, resp["ClientToken"])
			}
			if tt.wantPosixUser {
				_, hasPU := resp["PosixUser"]
				assert.True(t, hasPU)
			}
			if tt.wantRootDir {
				_, hasRD := resp["RootDirectory"]
				assert.True(t, hasRD)
			}
		})
	}
}

// TestAccessPoint_ClientTokenIdempotency_HTTP verifies that CreateAccessPoint with the
// same ClientToken returns the same access point on repeat calls, over HTTP. Real AWS
// implements this idempotency guarantee.
func TestAccessPoint_ClientTokenIdempotency_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "ap-idempotency",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	firstRec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"ClientToken":  "idem-token-123",
	})
	require.Equal(t, http.StatusOK, firstRec.Code)

	var firstOut struct {
		AccessPointID string `json:"AccessPointId"`
	}
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &firstOut))

	secondRec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"ClientToken":  "idem-token-123",
	})
	require.Equal(t, http.StatusOK, secondRec.Code)

	var secondOut struct {
		AccessPointID string `json:"AccessPointId"`
	}
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &secondOut))

	assert.Equal(t, firstOut.AccessPointID, secondOut.AccessPointID,
		"repeated CreateAccessPoint with same ClientToken must return the same access point")
}

// TestDeleteAccessPoint_CleansAPIndex verifies DeleteAccessPoint removes the apByFS
// index so DeleteFileSystem can proceed once the access point is gone.
func TestDeleteAccessPoint_CleansAPIndex(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID := createFS(t, h, "parity-ap-idx-cleanup")

	apRec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
		"ClientToken":  "tok-cleanup-1",
	})
	require.Equal(t, http.StatusOK, apRec.Code)

	var apOut map[string]any
	require.NoError(t, json.Unmarshal(apRec.Body.Bytes(), &apOut))
	apID := apOut["AccessPointId"].(string)

	blockRec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
	assert.Equal(t, http.StatusConflict, blockRec.Code, "delete blocked while AP exists")

	delAP := doREST(t, h, http.MethodDelete, "/2015-02-01/access-points/"+apID, nil)
	require.Equal(t, http.StatusNoContent, delAP.Code)

	delFS := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
	assert.Equal(t, http.StatusNoContent, delFS.Code,
		"delete should succeed after AP removed: %s", delFS.Body.String())
}
