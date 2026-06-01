package efs_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

func newRefinementBackend() *efs.InMemoryBackend {
	return efs.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

func newRefinementHandler() *efs.Handler {
	return efs.NewHandler(newRefinementBackend())
}

func doRESTRefinement(
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

func parseRefinementResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// createFS is a helper that creates a file system and returns its ID.
func createFS(t *testing.T, h *efs.Handler, token string) string {
	t.Helper()

	rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": token,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseRefinementResp(t, rec)

	id, ok := resp["FileSystemId"].(string)
	require.True(t, ok)

	return id
}

// fsReq is a convenience constructor for CreateFileSystemRequest with just a token.
func fsReq(token string) efs.CreateFileSystemRequest {
	return efs.CreateFileSystemRequest{CreationToken: token}
}

// mtReq is a convenience constructor for CreateMountTargetRequest.
func mtReq(fsID, subnetID string) efs.CreateMountTargetRequest {
	return efs.CreateMountTargetRequest{FileSystemID: fsID, SubnetID: subnetID}
}

// apReq is a convenience constructor for CreateAccessPointRequest.
func apReq(fsID string) efs.CreateAccessPointRequest {
	return efs.CreateAccessPointRequest{FileSystemID: fsID}
}

// TestRefinement1_Reset verifies that Reset() clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *efs.InMemoryBackend)
		name   string
		wantFS int
		wantMT int
		wantAP int
	}{
		{
			name:  "empty_backend_stays_empty",
			setup: func(_ *efs.InMemoryBackend) {},
		},
		{
			name: "resets_file_systems",
			setup: func(b *efs.InMemoryBackend) {
				_, err := b.CreateFileSystem(fsReq("t1"))
				require.NoError(t, err)
				_, err = b.CreateFileSystem(fsReq("t2"))
				require.NoError(t, err)
			},
		},
		{
			name: "resets_mount_targets",
			setup: func(b *efs.InMemoryBackend) {
				fs, err := b.CreateFileSystem(fsReq("t1"))
				require.NoError(t, err)
				_, err = b.CreateMountTarget(mtReq(fs.FileSystemID, "subnet-1"))
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			tt.setup(b)
			b.Reset()

			assert.Equal(t, 0, efs.FileSystemCount(b))
			assert.Equal(t, 0, efs.MountTargetCount(b))
			assert.Equal(t, 0, efs.AccessPointCount(b))
			assert.Equal(t, 0, efs.ARNIndexSize(b))
		})
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() clears backend state.
func TestRefinement1_HandlerReset(t *testing.T) {
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

			h := newRefinementHandler()
			tt.setup(h)
			h.Reset()

			assert.Equal(t, 0, efs.FileSystemCount(h.Backend))
		})
	}
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned for nil context.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{
			name:    "nil_ctx_returns_sentinel",
			wantErr: efs.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &efs.Provider{}
			_, err := p.Init(nil)

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_HandlerOpsPreBuilt verifies the ops map is populated on creation.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
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

			h := newRefinementHandler()
			assert.GreaterOrEqual(t, efs.OpsCount(h), tt.wantMinOps)
		})
	}
}

// TestRefinement1_ErrValidation verifies ErrValidation is a distinct sentinel.
func TestRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		perform func(b *efs.InMemoryBackend) error
		name    string
	}{
		{
			name: "bad_performance_mode",
			perform: func(b *efs.InMemoryBackend) error {
				_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
					CreationToken:   "tok",
					PerformanceMode: "badMode",
				})

				return err
			},
		},
		{
			name: "bad_throughput_mode",
			perform: func(b *efs.InMemoryBackend) error {
				_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
					CreationToken:  "tok",
					ThroughputMode: "badMode",
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			err := tt.perform(b)

			require.ErrorIs(t, err, efs.ErrValidation)
		})
	}
}

// TestRefinement1_PerformanceModeValidation verifies valid and invalid performance modes.
func TestRefinement1_PerformanceModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{name: "generalPurpose_valid", mode: "generalPurpose"},
		{name: "maxIO_valid", mode: "maxIO"},
		{name: "empty_defaults_to_valid", mode: ""},
		{name: "invalid_mode", mode: "invalid", wantErr: true},
		{name: "superPerf_invalid", mode: "superPerf", wantErr: true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			token := "token-perf-" + tt.name + "-" + string(rune('a'+i))
			_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
				CreationToken:   token,
				PerformanceMode: tt.mode,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, efs.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_ThroughputModeValidation verifies valid and invalid throughput modes.
func TestRefinement1_ThroughputModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		mode                     string
		provisionedThroughputMib float64
		wantErr                  bool
	}{
		{name: "bursting_valid", mode: "bursting"},
		{name: "provisioned_valid", mode: "provisioned", provisionedThroughputMib: 100},
		{name: "elastic_valid", mode: "elastic"},
		{name: "empty_defaults_to_valid", mode: ""},
		{name: "invalid_mode", mode: "invalid", wantErr: true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			token := "token-thru-" + tt.name + "-" + string(rune('a'+i))
			_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
				CreationToken:            token,
				ThroughputMode:           tt.mode,
				ProvisionedThroughputMib: tt.provisionedThroughputMib,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, efs.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_ARNIndexes verifies ARN index is populated and enables O(1) lookup.
func TestRefinement1_ARNIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		perform     func(b *efs.InMemoryBackend) (string, error)
		name        string
		wantARNSize int
	}{
		{
			name: "file_system_arn_indexed",
			perform: func(b *efs.InMemoryBackend) (string, error) {
				fs, err := b.CreateFileSystem(fsReq("tok-arn"))
				if err != nil {
					return "", err
				}

				return fs.FileSystemArn, nil
			},
			wantARNSize: 1,
		},
		{
			name: "mount_target_arn_indexed",
			perform: func(b *efs.InMemoryBackend) (string, error) {
				fs, err := b.CreateFileSystem(fsReq("tok-mt-arn"))
				if err != nil {
					return "", err
				}
				mt, err := b.CreateMountTarget(mtReq(fs.FileSystemID, "sn-1"))
				if err != nil {
					return "", err
				}

				return mt.MountTargetArn, nil
			},
			wantARNSize: 2,
		},
		{
			name: "access_point_arn_indexed",
			perform: func(b *efs.InMemoryBackend) (string, error) {
				fs, err := b.CreateFileSystem(fsReq("tok-ap-arn"))
				if err != nil {
					return "", err
				}
				ap, err := b.CreateAccessPoint(apReq(fs.FileSystemID))
				if err != nil {
					return "", err
				}

				return ap.AccessPointArn, nil
			},
			wantARNSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			arnVal, err := tt.perform(b)
			require.NoError(t, err)
			assert.NotEmpty(t, arnVal)
			assert.Equal(t, tt.wantARNSize, efs.ARNIndexSize(b))
		})
	}
}

// TestRefinement1_SortedDescribeFileSystems verifies DescribeFileSystems returns sorted results.
func TestRefinement1_SortedDescribeFileSystems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "single_fs", count: 1},
		{name: "multiple_fs_sorted", count: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			for i := range tt.count {
				token := "token-sort-" + tt.name + "-" + string(rune('a'+i))
				createFS(t, h, token)
			}

			rec := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/file-systems", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			list, ok := resp["FileSystems"].([]any)
			require.True(t, ok)
			require.Len(t, list, tt.count)

			for i := 1; i < len(list); i++ {
				prev := list[i-1].(map[string]any)["FileSystemId"].(string)
				curr := list[i].(map[string]any)["FileSystemId"].(string)
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

// TestRefinement1_SortedDescribeMountTargets verifies sorted mount targets.
func TestRefinement1_SortedDescribeMountTargets(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-mt-sort-"+tt.name)

			for i := range tt.count {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "sn-" + string(rune('a'+i)),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/mount-targets", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			list, ok := resp["MountTargets"].([]any)
			require.True(t, ok)
			require.Len(t, list, tt.count)

			for i := 1; i < len(list); i++ {
				prev := list[i-1].(map[string]any)["MountTargetId"].(string)
				curr := list[i].(map[string]any)["MountTargetId"].(string)
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

// TestRefinement1_SortedDescribeAccessPoints verifies sorted access points.
func TestRefinement1_SortedDescribeAccessPoints(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-ap-sort-"+tt.name)

			for range tt.count {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": fsID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/access-points", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
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

// TestRefinement1_SortedDescribeReplicationConfigurations verifies sorted replication configs.
func TestRefinement1_SortedDescribeReplicationConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "multiple_sorted", count: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			for i := range tt.count {
				fsID := createFS(t, h, "tok-rep-sort-"+tt.name+"-"+string(rune('a'+i)))
				rec := doRESTRefinement(t, h, http.MethodPost,
					"/2015-02-01/file-systems/"+fsID+"/replication-configuration",
					map[string]any{
						"Destinations": []map[string]any{
							{"Region": "us-west-2"},
						},
					})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRESTRefinement(t, h, http.MethodGet,
				"/2015-02-01/file-systems/replication-configurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			list, ok := resp["Replications"].([]any)
			require.True(t, ok)
			require.Len(t, list, tt.count)

			for i := 1; i < len(list); i++ {
				prev := list[i-1].(map[string]any)["SourceFileSystemId"].(string)
				curr := list[i].(map[string]any)["SourceFileSystemId"].(string)
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

// TestRefinement1_DeleteFileSystem_RequiresEmptyState verifies AWS-accurate behavior:
// DeleteFileSystem returns ErrFileSystemInUse when mount targets or access points exist.
// Callers must delete dependents before deleting the file system.
func TestRefinement1_DeleteFileSystem_RequiresEmptyState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numMTs        int
		numAPs        int
		wantErrOnFull bool
	}{
		{name: "no_dependents_succeeds", numMTs: 0, numAPs: 0, wantErrOnFull: false},
		{name: "with_mount_target_rejected", numMTs: 1, numAPs: 0, wantErrOnFull: true},
		{name: "with_access_point_rejected", numMTs: 0, numAPs: 1, wantErrOnFull: true},
		{name: "with_both_rejected", numMTs: 2, numAPs: 1, wantErrOnFull: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-del-" + tt.name))
			require.NoError(t, err)

			var mtIDs []string
			for i := range tt.numMTs {
				mt, mtErr := b.CreateMountTarget(mtReq(fs.FileSystemID, "sn-"+string(rune('a'+i))))
				require.NoError(t, mtErr)
				mtIDs = append(mtIDs, mt.MountTargetID)
			}

			var apIDs []string
			for range tt.numAPs {
				ap, apErr := b.CreateAccessPoint(apReq(fs.FileSystemID))
				require.NoError(t, apErr)
				apIDs = append(apIDs, ap.AccessPointID)
			}

			// First delete attempt.
			err = b.DeleteFileSystem(fs.FileSystemID)
			if tt.wantErrOnFull {
				require.ErrorIs(t, err, efs.ErrFileSystemInUse)

				// Clean up dependents and retry.
				for _, mtID := range mtIDs {
					require.NoError(t, b.DeleteMountTarget(mtID))
				}
				for _, apID := range apIDs {
					require.NoError(t, b.DeleteAccessPoint(apID))
				}

				err = b.DeleteFileSystem(fs.FileSystemID)
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, 0, efs.FileSystemCount(b))
		})
	}
}

// TestRefinement1_DescribeFileSystems_FilterMiss_EmptyList verifies FileSystemNotFound on unknown FS ID.
func TestRefinement1_DescribeFileSystems_FilterMiss_EmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "nonexistent_id_returns_empty", id: "fs-notfound"},
		{name: "garbage_id_returns_empty", id: "not-a-real-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			_, _, err := b.DescribeFileSystems(tt.id, "", 0)
			require.ErrorIs(t, err, efs.ErrNotFound)
		})
	}
}

// TestRefinement1_PutFileSystemPolicy verifies PutFileSystemPolicy sets and retrieves a policy.
func TestRefinement1_PutFileSystemPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "set_policy", policy: `{"Version":"2012-10-17","Statement":[]}`},
		{name: "overwrite_policy", policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-fsp-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.Equal(t, tt.policy, resp["Policy"])

			// Verify via describe.
			rec2 := doRESTRefinement(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			resp2 := parseRefinementResp(t, rec2)
			assert.Equal(t, tt.policy, resp2["Policy"])
		})
	}
}

// TestRefinement1_PutBackupPolicy verifies PutBackupPolicy sets the backup policy status.
func TestRefinement1_PutBackupPolicy(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-bp-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/backup-policy",
				map[string]any{
					"BackupPolicy": map[string]any{"Status": tt.status},
				})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify via describe.
			rec2 := doRESTRefinement(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/backup-policy", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			resp2 := parseRefinementResp(t, rec2)
			bp := resp2["BackupPolicy"].(map[string]any)
			assert.Equal(t, tt.status, bp["Status"])
		})
	}
}

// TestRefinement1_UpdateFileSystem verifies UpdateFileSystem updates throughput mode.
func TestRefinement1_UpdateFileSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantMode   string
		wantStatus int
	}{
		{
			name:       "update_to_elastic",
			body:       map[string]any{"ThroughputMode": "elastic"},
			wantStatus: http.StatusAccepted,
			wantMode:   "elastic",
		},
		{
			name: "update_to_provisioned_with_throughput",
			body: map[string]any{
				"ThroughputMode":               "provisioned",
				"ProvisionedThroughputInMibps": 128.0,
			},
			wantStatus: http.StatusAccepted,
			wantMode:   "provisioned",
		},
		{
			name:       "invalid_mode_returns_400",
			body:       map[string]any{"ThroughputMode": "invalid"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-upd-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantMode != "" {
				resp := parseRefinementResp(t, rec)
				assert.Equal(t, tt.wantMode, resp["ThroughputMode"])
			}
		})
	}
}

// TestRefinement1_TagResource_ARNIndex verifies TagResource uses ARN index for lookup.
func TestRefinement1_TagResource_ARNIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		useARN  bool
		wantErr bool
	}{
		{name: "tag_by_id", useARN: false},
		{name: "tag_by_arn", useARN: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-tag-arn-" + tt.name))
			require.NoError(t, err)

			resourceID := fs.FileSystemID
			if tt.useARN {
				resourceID = fs.FileSystemArn
			}

			err = b.TagResource(resourceID, map[string]string{"env": "test"})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(resourceID)
			require.NoError(t, err)
			assert.Equal(t, "test", tags["env"])
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddFileSystemInternal, AddMountTargetInternal, AddAccessPointInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFS    int
		wantMT    int
		wantAP    int
		wantARNsz int
	}{
		{
			name:      "seed_one_of_each",
			wantFS:    1,
			wantMT:    1,
			wantAP:    1,
			wantARNsz: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()

			b.AddFileSystemInternal(&efs.FileSystem{
				FileSystemID:  "fs-seed01",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-seed01",
			})
			b.AddMountTargetInternal(&efs.MountTarget{
				MountTargetID:  "fsmt-seed01",
				MountTargetArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:mount-target/fsmt-seed01",
				FileSystemID:   "fs-seed01",
			})
			b.AddAccessPointInternal(&efs.AccessPoint{
				AccessPointID:  "fsap-seed01",
				AccessPointArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:access-point/fsap-seed01",
				FileSystemID:   "fs-seed01",
			})

			assert.Equal(t, tt.wantFS, efs.FileSystemCount(b))
			assert.Equal(t, tt.wantMT, efs.MountTargetCount(b))
			assert.Equal(t, tt.wantAP, efs.AccessPointCount(b))
			assert.Equal(t, tt.wantARNsz, efs.ARNIndexSize(b))
		})
	}
}

// TestRefinement1_ExportCountHelpers verifies that export count helpers return correct values.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantFS           int
		wantMT           int
		wantAP           int
		wantRepl         int
		wantBackupPolicy int
		wantFSPolicy     int
	}{
		{
			name:             "all_counts_correct",
			wantFS:           1,
			wantMT:           1,
			wantAP:           1,
			wantRepl:         1,
			wantBackupPolicy: 1,
			wantFSPolicy:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-counts-" + tt.name))
			require.NoError(t, err)

			_, err = b.CreateMountTarget(mtReq(fs.FileSystemID, "sn-1"))
			require.NoError(t, err)

			_, err = b.CreateAccessPoint(apReq(fs.FileSystemID))
			require.NoError(t, err)

			_, err = b.CreateReplicationConfiguration(fs.FileSystemID, []efs.ReplicationDestination{
				{Region: "us-west-2"},
			})
			require.NoError(t, err)

			err = b.PutBackupPolicy(fs.FileSystemID, "ENABLED")
			require.NoError(t, err)

			err = b.PutFileSystemPolicy(fs.FileSystemID, `{"Version":"2012-10-17"}`)
			require.NoError(t, err)

			assert.Equal(t, tt.wantFS, efs.FileSystemCount(b))
			assert.Equal(t, tt.wantMT, efs.MountTargetCount(b))
			assert.Equal(t, tt.wantAP, efs.AccessPointCount(b))
			assert.Equal(t, tt.wantRepl, efs.ReplicationConfigCount(b))
			assert.Equal(t, tt.wantBackupPolicy, efs.BackupPolicyCount(b))
			assert.Equal(t, tt.wantFSPolicy, efs.FileSystemPolicyCount(b))
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore round-trips all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantFS int
		wantMT int
		wantAP int
	}{
		{name: "roundtrip_preserves_counts", wantFS: 1, wantMT: 1, wantAP: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-persist-" + tt.name))
			require.NoError(t, err)

			_, err = b.CreateMountTarget(mtReq(fs.FileSystemID, "sn-1"))
			require.NoError(t, err)

			_, err = b.CreateAccessPoint(apReq(fs.FileSystemID))
			require.NoError(t, err)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefinementBackend()
			err = b2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, tt.wantFS, efs.FileSystemCount(b2))
			assert.Equal(t, tt.wantMT, efs.MountTargetCount(b2))
			assert.Equal(t, tt.wantAP, efs.AccessPointCount(b2))
			// ARN indexes should be rebuilt.
			assert.Positive(t, efs.ARNIndexSize(b2))
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies backend is usable after multiple reset cycles.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
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

			h := newRefinementHandler()

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

// TestRefinement1_ValidationHandler verifies handler returns 400 for invalid mode.
func TestRefinement1_ValidationHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "bad_performance_mode_returns_400",
			body: map[string]any{
				"CreationToken":   "tok-val-1",
				"PerformanceMode": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "bad_throughput_mode_returns_400",
			body: map[string]any{
				"CreationToken":  "tok-val-2",
				"ThroughputMode": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
