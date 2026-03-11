package efs_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

func newTestEFSHandler() *efs.Handler {
	backend := efs.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return efs.NewHandler(backend)
}

func doREST(
	t *testing.T,
	h *efs.Handler,
	method, path string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestFileSystemCRUD exercises CreateFileSystem, DescribeFileSystems and DeleteFileSystem.
func TestFileSystemCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "create_and_describe",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken":   "my-token",
					"PerformanceMode": "generalPurpose",
					"Tags":            []map[string]string{{"Key": "Name", "Value": "my-fs"}},
				})
				assert.Equal(t, http.StatusCreated, rec.Code)
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["FileSystemId"])
				assert.Equal(t, "available", resp["LifeCycleState"])
				assert.Equal(t, "my-token", resp["CreationToken"])

				// Describe all.
				rec2 := doREST(t, h, http.MethodGet, "/2015-02-01/file-systems", nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResp(t, rec2)
				list := resp2["FileSystems"].([]any)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "create_with_empty_token_returns_400",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_file_system",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "del-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				resp := parseResp(t, rec)
				fsID := resp["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
				assert.Equal(t, http.StatusNoContent, rec2.Code)

				// Describe after delete returns not found.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/file-systems/"+fsID, nil)
				assert.Equal(t, http.StatusNotFound, rec3.Code)
			},
		},
		{
			name: "delete_non_existent_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/fs-notexist", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_duplicate_token_returns_409",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "dup-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				// Second create with same token should return 409.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "dup-token",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
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

// TestMountTargetCRUD exercises CreateMountTarget, DescribeMountTargets and DeleteMountTarget.
func TestMountTargetCRUD(t *testing.T) {
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
					"CreationToken": "mt-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// Create mount target.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-abc123",
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				mt := parseResp(t, rec2)
				assert.Equal(t, fsID, mt["FileSystemId"])
				mtID := mt["MountTargetId"].(string)

				// Describe all.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets", nil)
				assert.Equal(t, http.StatusOK, rec3.Code)
				list := parseResp(t, rec3)["MountTargets"].([]any)
				assert.Len(t, list, 1)

				// Delete mount target.
				rec4 := doREST(t, h, http.MethodDelete, "/2015-02-01/mount-targets/"+mtID, nil)
				assert.Equal(t, http.StatusNoContent, rec4.Code)
			},
		},
		{
			name: "create_mount_target_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": "fs-notexist",
					"SubnetId":     "subnet-abc",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_mount_target_missing_filesystem_id_returns_400",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": "",
					"SubnetId":     "subnet-abc",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_non_existent_mount_target_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/2015-02-01/mount-targets/fsmt-notexist", nil)
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

// TestTagOperations exercises TagResource and ListTagsForResource.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "tag_and_list_file_system",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "tag-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// Tag the resource.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/tags/"+fsID, map[string]any{
					"Tags": []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// List tags.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+fsID, nil)
				assert.Equal(t, http.StatusOK, rec3.Code)
				resp := parseResp(t, rec3)
				tagsList := resp["Tags"].([]any)
				assert.NotEmpty(t, tagsList)
			},
		},
		{
			name: "list_tags_non_existent_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/2015-02-01/tags/fs-notexist", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "tag_non_existent_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/tags/fs-notexist", map[string]any{
					"Tags": []map[string]string{{"Key": "k", "Value": "v"}},
				})
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

// TestHandlerMeta tests handler metadata methods.
func TestHandlerMeta(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	tests := []struct {
		want any
		fn   func() any
		name string
	}{
		{
			name: "name",
			fn:   func() any { return h.Name() },
			want: "EFS",
		},
		{
			name: "chaos_service_name",
			fn:   func() any { return h.ChaosServiceName() },
			want: "efs",
		},
		{
			name: "supported_operations_not_empty",
			fn:   func() any { return len(h.GetSupportedOperations()) > 0 },
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.fn())
		})
	}
}

// TestUnknownOperation verifies that unknown routes return 404.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "unknown_path",
			method: http.MethodGet,
			path:   "/2015-02-01/unknown-resource",
		},
		{
			name:   "patch_file_system",
			method: http.MethodPatch,
			path:   "/2015-02-01/file-systems",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doREST(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandlerRouteMatching tests RouteMatcher, ExtractOperation and ExtractResource.
func TestHandlerRouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	e := echo.New()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
		wantResource  string
		wantMatch     bool
	}{
		{
			name:          "create_file_system",
			method:        http.MethodPost,
			path:          "/2015-02-01/file-systems",
			wantOperation: "CreateFileSystem",
			wantMatch:     true,
		},
		{
			name:          "describe_file_systems",
			method:        http.MethodGet,
			path:          "/2015-02-01/file-systems",
			wantOperation: "DescribeFileSystems",
			wantMatch:     true,
		},
		{
			name:          "delete_file_system",
			method:        http.MethodDelete,
			path:          "/2015-02-01/file-systems/fs-12345678",
			wantOperation: "DeleteFileSystem",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			name:          "create_mount_target",
			method:        http.MethodPost,
			path:          "/2015-02-01/mount-targets",
			wantOperation: "CreateMountTarget",
			wantMatch:     true,
		},
		{
			name:          "delete_mount_target",
			method:        http.MethodDelete,
			path:          "/2015-02-01/mount-targets/fsmt-abc",
			wantOperation: "DeleteMountTarget",
			wantResource:  "fsmt-abc",
			wantMatch:     true,
		},
		{
			name:          "describe_mount_targets",
			method:        http.MethodGet,
			path:          "/2015-02-01/mount-targets",
			wantOperation: "DescribeMountTargets",
			wantMatch:     true,
		},
		{
			name:          "create_access_point",
			method:        http.MethodPost,
			path:          "/2015-02-01/access-points",
			wantOperation: "CreateAccessPoint",
			wantMatch:     true,
		},
		{
			name:          "delete_access_point",
			method:        http.MethodDelete,
			path:          "/2015-02-01/access-points/fsap-abc",
			wantOperation: "DeleteAccessPoint",
			wantResource:  "fsap-abc",
			wantMatch:     true,
		},
		{
			name:          "tag_resource",
			method:        http.MethodPost,
			path:          "/2015-02-01/tags/fs-12345678",
			wantOperation: "TagResource",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			name:          "list_tags",
			method:        http.MethodGet,
			path:          "/2015-02-01/tags/fs-12345678",
			wantOperation: "ListTagsForResource",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			name:      "non_efs_path_no_match",
			method:    http.MethodGet,
			path:      "/some-other-service",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))

			if tt.wantOperation != "" {
				assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			}
			if tt.wantResource != "" {
				assert.Equal(t, tt.wantResource, h.ExtractResource(c))
			}
		})
	}
}

// TestHandlerChaos tests chaos-related methods.
func TestHandlerChaos(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	tests := []struct {
		want any
		fn   func() any
		name string
	}{
		{
			name: "chaos_operations_match_supported",
			fn:   func() any { return h.ChaosOperations() },
			want: h.GetSupportedOperations(),
		},
		{
			name: "chaos_regions_has_default",
			fn: func() any {
				regions := h.ChaosRegions()

				return len(regions) > 0
			},
			want: true,
		},
		{
			name: "match_priority_is_positive",
			fn:   func() any { return h.MatchPriority() > 0 },
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.fn())
		})
	}
}

// TestBackendRegion tests the Region method.
func TestBackendRegion(t *testing.T) {
	t.Parallel()

	backend := efs.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", backend.Region())
}

// TestTagResourceByARN tests tagging via ARN instead of ID.
func TestTagResourceByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)
	fsARN := resp["FileSystemArn"].(string)
	require.NotEmpty(t, fsARN)

	// Tag via ARN.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/tags/"+fsARN, map[string]any{
		"Tags": []map[string]string{{"Key": "tagged", "Value": "true"}},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List tags via ARN.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+fsARN, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestDescribeMountTargetByID tests describing a specific mount target by ID.
func TestDescribeMountTargetByID(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "mt-id-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create mount target.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-abc",
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	mtID := parseResp(t, rec2)["MountTargetId"].(string)

	// Describe by ID via path.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets/"+mtID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	list := parseResp(t, rec3)["MountTargets"].([]any)
	assert.Len(t, list, 1)
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

// TestTagAccessPointByARN tests tagging access points via ARN.
func TestTagAccessPointByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "ap-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create access point.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	apARN := parseResp(t, rec2)["AccessPointArn"].(string)
	require.NotEmpty(t, apARN)

	// Tag via ARN.
	rec3 := doREST(t, h, http.MethodPost, "/2015-02-01/tags/"+apARN, map[string]any{
		"Tags": []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestTagResourceByPercentEncodedARN tests tagging with a percent-encoded ARN.
func TestTagResourceByPercentEncodedARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "pct-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)
	fsARN := resp["FileSystemArn"].(string)
	require.NotEmpty(t, fsARN)

	// Tag via percent-encoded ARN in path (simulating SDK/Terraform behaviour).
	encodedARN := url.PathEscape(fsARN)
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/tags/"+encodedARN, map[string]any{
		"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List tags via percent-encoded ARN.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+encodedARN, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	tagsResp := parseResp(t, rec3)
	tagsRaw, ok := tagsResp["Tags"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, tagsRaw)
}

// TestListTagsForAccessPointByARN tests that ListTagsForResource works with an access point ARN.
func TestListTagsForAccessPointByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "lt-ap-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create access point.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	apResp := parseResp(t, rec2)
	apARN := apResp["AccessPointArn"].(string)
	require.NotEmpty(t, apARN)

	// Tag the access point via its ARN.
	rec3 := doREST(t, h, http.MethodPost, "/2015-02-01/tags/"+apARN, map[string]any{
		"Tags": []map[string]string{{"Key": "purpose", "Value": "e2e"}},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// List tags for the access point via ARN.
	rec4 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+apARN, nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
	tagsResp := parseResp(t, rec4)
	tagsRaw, ok := tagsResp["Tags"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, tagsRaw)
}
