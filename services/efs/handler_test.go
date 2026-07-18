package efs_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

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

// TestHandlerOpsPreBuilt verifies the ops map is populated on creation.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantMinOps int
	}{
		{
			name:       "ops_pre_built_on_new_handler",
			wantMinOps: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			assert.GreaterOrEqual(t, efs.OpsCount(h), tt.wantMinOps)
		})
	}
}

// TestHandlerReset verifies that Handler.Reset() clears backend state.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(h *efs.Handler)
		name  string
	}{
		{
			name:  "resets_via_handler",
			setup: func(h *efs.Handler) { createFS(t, h, "token-1") },
		},
		{
			name:  "multiple_reset_cycles",
			setup: func(_ *efs.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			tt.setup(h)
			h.Reset()

			assert.Equal(t, 0, efs.FileSystemCount(h.Backend))
		})
	}
}

// TestMultipleResetCycle verifies the backend is usable after multiple reset cycles.
func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cycles int
	}{
		{name: "three_cycles", cycles: 3},
		{name: "single_cycle", cycles: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()

			for range tt.cycles {
				createFS(t, h, "tok-cycle-"+tt.name+"-cycle")
				h.Reset()
				assert.Equal(t, 0, efs.FileSystemCount(h.Backend))
			}

			// After all cycles, should still work normally.
			id := createFS(t, h, "tok-final-"+tt.name)
			assert.NotEmpty(t, id)
			assert.Equal(t, 1, efs.FileSystemCount(h.Backend))
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
			// Real aws-sdk-go-v2 sends TagResource/UntagResource/ListTagsForResource
			// to "/2015-02-01/resource-tags/{ResourceId}" (see serializers.go in
			// aws-sdk-go-v2/service/efs), NOT "/2015-02-01/tags/{id}" -- that path is
			// reserved for the deprecated, GET-only DescribeTags op. Routing these
			// under "/2015-02-01/tags/" (as this handler previously did) makes them
			// unreachable by real SDK clients.
			name:          "tag_resource",
			method:        http.MethodPost,
			path:          "/2015-02-01/resource-tags/fs-12345678",
			wantOperation: "TagResource",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			name:          "list_tags",
			method:        http.MethodGet,
			path:          "/2015-02-01/resource-tags/fs-12345678",
			wantOperation: "ListTagsForResource",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			name:          "untag_resource",
			method:        http.MethodDelete,
			path:          "/2015-02-01/resource-tags/fs-12345678",
			wantOperation: "UntagResource",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			// The legacy DescribeTags op is GET-only on "/2015-02-01/tags/{FileSystemId}".
			name:          "describe_tags_legacy",
			method:        http.MethodGet,
			path:          "/2015-02-01/tags/fs-12345678",
			wantOperation: "DescribeTags",
			wantResource:  "fs-12345678",
			wantMatch:     true,
		},
		{
			// POST is not bound to any op at the legacy DescribeTags path.
			name:          "tags_legacy_path_post_unmatched_operation",
			method:        http.MethodPost,
			path:          "/2015-02-01/tags/fs-12345678",
			wantOperation: "Unknown",
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

// TestErrorBodyShape verifies x-amzn-ErrorType header is set on errors, across
// several different error kinds routed through different operations.
func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		method        string
		path          string
		wantErrorType string
		wantStatus    int
	}{
		{
			name:          "not_found_sets_header",
			method:        http.MethodGet,
			path:          "/2015-02-01/file-systems/fs-notexist",
			wantStatus:    http.StatusNotFound,
			wantErrorType: "FileSystemNotFound",
		},
		{
			name:          "bad_performance_mode_sets_header",
			method:        http.MethodPost,
			path:          "/2015-02-01/file-systems",
			body:          map[string]any{"CreationToken": "tok-err", "PerformanceMode": "invalid"},
			wantStatus:    http.StatusBadRequest,
			wantErrorType: "ValidationException",
		},
		{
			name:          "delete_fs_with_mount_target_sets_header",
			method:        "", // handled separately
			path:          "",
			wantStatus:    http.StatusConflict,
			wantErrorType: "FileSystemInUse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "delete_fs_with_mount_target_sets_header" {
				h := newTestEFSHandler()
				fsID := createFS(t, h, "tok-err-hdr-del")
				doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "sn-abc",
				})

				rec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
				assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-Errortype"))

				return
			}

			h := newTestEFSHandler()
			rec := doREST(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-Errortype"))
		})
	}
}
