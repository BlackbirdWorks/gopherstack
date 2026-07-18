package efs_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

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

				// Describe after delete returns 404 FileSystemNotFound (AWS behaviour).
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
			name: "create_duplicate_token_identical_args_returns_200",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "dup-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				firstID := parseResp(t, rec)["FileSystemId"].(string)

				// Second create with same token and identical args returns existing FS with 200.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "dup-token",
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				secondID := parseResp(t, rec2)["FileSystemId"].(string)
				assert.Equal(t, firstID, secondID)
			},
		},
		{
			name: "create_duplicate_token_different_args_returns_409",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken":   "dup-token2",
					"PerformanceMode": "generalPurpose",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				// Same token but different PerformanceMode returns 409 FileSystemAlreadyExists.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken":   "dup-token2",
					"PerformanceMode": "maxIO",
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

// TestCreationTokenMaxLength verifies CreationToken is rejected when longer than 64 chars.
func TestCreationTokenMaxLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{
			name:       "token_exactly_64_chars_accepted",
			token:      strings.Repeat("a", 64),
			wantStatus: http.StatusCreated,
		},
		{
			name:       "token_65_chars_rejected",
			token:      strings.Repeat("a", 65),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "token_100_chars_rejected",
			token:      strings.Repeat("x", 100),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "token_1_char_accepted",
			token:      "a",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			rec := doREST(
				t,
				h,
				http.MethodPost,
				"/2015-02-01/file-systems",
				map[string]any{
					"CreationToken": tt.token,
				},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				resp := parseResp(t, rec)
				assert.Contains(t, resp["ErrorCode"], "ValidationException")
			}
		})
	}
}

// TestDescribeFileSystems_NotFound_HTTP verifies DescribeFileSystems returns 404
// when a specific FileSystemId is requested but does not exist.
func TestDescribeFileSystems_NotFound_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fsID       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "nonexistent_id_returns_404",
			fsID:       "fs-notfound",
			wantStatus: http.StatusNotFound,
			wantErr:    "FileSystemNotFound",
		},
		{
			name:       "garbage_id_returns_404",
			fsID:       "not-a-real-id",
			wantStatus: http.StatusNotFound,
			wantErr:    "FileSystemNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			rec := doREST(t, h, http.MethodGet, "/2015-02-01/file-systems/"+tt.fsID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErr, resp["ErrorCode"])
		})
	}
}

// TestDescribeFileSystems_CreationTokenFilter verifies that ?CreationToken= query param
// filters DescribeFileSystems to the matching file system, matching AWS EFS behaviour.
func TestDescribeFileSystems_CreationTokenFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		token        string
		setupTokens  []string
		wantCount    int
		wantStatus   int
		wantNotFound bool
	}{
		{
			name:        "existing_token_returns_matching_fs",
			setupTokens: []string{"tok-alpha", "tok-beta"},
			token:       "tok-alpha",
			wantStatus:  http.StatusOK,
			wantCount:   1,
		},
		{
			name:        "nonexistent_token_returns_empty_list",
			setupTokens: []string{"tok-gamma"},
			token:       "tok-nonexistent",
			wantStatus:  http.StatusOK,
			wantCount:   0,
		},
		{
			name:        "no_token_returns_all",
			setupTokens: []string{"tok-one", "tok-two", "tok-three"},
			token:       "",
			wantStatus:  http.StatusOK,
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			for _, tok := range tt.setupTokens {
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/file-systems",
					map[string]any{
						"CreationToken": tok,
					},
				)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/2015-02-01/file-systems"
			if tt.token != "" {
				path += "?CreationToken=" + tt.token
			}

			rec := doREST(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResp(t, rec)
			fsList, _ := resp["FileSystems"].([]any)
			assert.Len(t, fsList, tt.wantCount)

			if tt.wantCount == 1 && tt.token != "" {
				fs := fsList[0].(map[string]any)
				assert.Equal(t, tt.token, fs["CreationToken"])
			}
		})
	}
}

// TestSortedDescribeFileSystems verifies DescribeFileSystems returns sorted results.
func TestSortedDescribeFileSystems(t *testing.T) {
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

			h := newTestEFSHandler()
			for i := range tt.count {
				token := "token-sort-" + tt.name + "-" + string(rune('a'+i))
				createFS(t, h, token)
			}

			rec := doREST(t, h, http.MethodGet, "/2015-02-01/file-systems", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
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

// TestUpdateFileSystem verifies UpdateFileSystem updates throughput mode.
func TestUpdateFileSystem(t *testing.T) {
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

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-upd-"+tt.name)

			rec := doREST(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantMode != "" {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantMode, resp["ThroughputMode"])
			}
		})
	}
}

// TestValidationHandler verifies the handler returns 400 for invalid create-time modes.
func TestValidationHandler(t *testing.T) {
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

			h := newTestEFSHandler()
			rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCreationTokenIdempotency_HTTP verifies handler HTTP status codes.
func TestCreationTokenIdempotency_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		first      map[string]any
		second     map[string]any
		name       string
		wantFirst  int
		wantSecond int
	}{
		{
			name:       "identical_args_returns_200",
			first:      map[string]any{"CreationToken": "http-tok1"},
			second:     map[string]any{"CreationToken": "http-tok1"},
			wantFirst:  http.StatusCreated,
			wantSecond: http.StatusOK,
		},
		{
			name: "different_perf_mode_returns_409",
			first: map[string]any{
				"CreationToken":   "http-tok2",
				"PerformanceMode": "generalPurpose",
			},
			second:     map[string]any{"CreationToken": "http-tok2", "PerformanceMode": "maxIO"},
			wantFirst:  http.StatusCreated,
			wantSecond: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			rec1 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.first)
			assert.Equal(t, tt.wantFirst, rec1.Code)

			rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.second)
			assert.Equal(t, tt.wantSecond, rec2.Code)
		})
	}
}

// TestProvisionedThroughput_InResponse verifies the field appears in the HTTP response.
func TestProvisionedThroughput_InResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantMibps  float64
		wantInResp bool
	}{
		{
			name: "provisioned_mode_emits_throughput",
			body: map[string]any{
				"CreationToken":                "prov-resp",
				"ThroughputMode":               "provisioned",
				"ProvisionedThroughputInMibps": 512.0,
			},
			wantMibps:  512.0,
			wantInResp: true,
		},
		{
			name: "bursting_mode_omits_throughput",
			body: map[string]any{
				"CreationToken":  "burst-resp",
				"ThroughputMode": "bursting",
			},
			wantInResp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			resp := parseResp(t, rec)
			_, hasField := resp["ProvisionedThroughputInMibps"]
			assert.Equal(t, tt.wantInResp, hasField)

			if tt.wantInResp {
				assert.InDelta(
					t,
					tt.wantMibps,
					resp["ProvisionedThroughputInMibps"].(float64),
					0.001,
				)
			}
		})
	}
}

// TestDeleteFileSystem_FileSystemInUse verifies the correct error and HTTP status.
func TestDeleteFileSystem_FileSystemInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantErrCode    string
		wantHTTPStatus int
		createMT       bool
		createAP       bool
	}{
		{
			name:           "no_deps_delete_succeeds",
			wantHTTPStatus: http.StatusNoContent,
		},
		{
			name:           "has_mount_target_returns_409",
			createMT:       true,
			wantHTTPStatus: http.StatusConflict,
			wantErrCode:    "FileSystemInUse",
		},
		{
			name:           "has_access_point_returns_409",
			createAP:       true,
			wantHTTPStatus: http.StatusConflict,
			wantErrCode:    "FileSystemInUse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-del-inuse-"+tt.name)

			if tt.createMT {
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/mount-targets",
					map[string]any{
						"FileSystemId": fsID,
						"SubnetId":     "subnet-abc",
					},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}
			if tt.createAP {
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

			rec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if tt.wantErrCode != "" {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantErrCode, resp["ErrorCode"])
			}
		})
	}
}

// TestSizeInBytes_IncludesStorageClassBreakdown verifies that DescribeFileSystems
// returns ValueInIA, ValueInStandard, and ValueInArchive inside SizeInBytes. Real AWS
// includes all three storage-class breakdown fields alongside Value.
func TestSizeInBytes_IncludesStorageClassBreakdown(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "sizebreakdown-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code, "CreateFileSystem failed: %s", rec.Body.String())

	var createOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

	descRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?FileSystemId="+createOut.FileSystemID, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		FileSystems []struct {
			SizeInBytes map[string]any `json:"SizeInBytes"`
		} `json:"FileSystems"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.Len(t, descOut.FileSystems, 1)

	sib := descOut.FileSystems[0].SizeInBytes
	require.NotNil(t, sib, "SizeInBytes must be present")

	_, hasValueInIA := sib["ValueInIA"]
	assert.True(t, hasValueInIA, "SizeInBytes must include ValueInIA")

	_, hasValueInStandard := sib["ValueInStandard"]
	assert.True(t, hasValueInStandard, "SizeInBytes must include ValueInStandard")

	_, hasValueInArchive := sib["ValueInArchive"]
	assert.True(t, hasValueInArchive, "SizeInBytes must include ValueInArchive")
}

// TestDeleteFileSystem_RejectedWhenMountTargetsExist verifies that DeleteFileSystem
// returns 409 FileSystemInUse when mount targets exist. Real AWS enforces this.
func TestDeleteFileSystem_RejectedWhenMountTargetsExist(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "del-fs-with-mt",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	mtRec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"SubnetId":     "subnet-block-delete",
	})
	require.Equal(t, http.StatusOK, mtRec.Code)

	rec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsOut.FileSystemID, nil)

	assert.Equal(t, http.StatusConflict, rec.Code,
		"DeleteFileSystem with existing mount targets must return 409; body: %s", rec.Body.String())
}

// TestFileSystem_CreatingToAvailableLifecycle verifies the file system lifecycle
// transitions from "creating" to "available" when a non-zero activation delay is set.
func TestFileSystem_CreatingToAvailableLifecycle(t *testing.T) {
	t.Parallel()

	b := efs.NewInMemoryBackend("123456789012", "us-east-1")
	efs.SetFSActivationDelay(b, 80*time.Millisecond)
	h := efs.NewHandler(b)

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "parity-lifecycle-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "creating", out["LifeCycleState"],
		"newly created FS should be in 'creating' state immediately after CreateFileSystem")

	fsID := out["FileSystemId"].(string)

	require.Eventually(t, func() bool {
		descRec := doREST(t, h, http.MethodGet,
			"/2015-02-01/file-systems?FileSystemId="+fsID, nil)
		if descRec.Code != http.StatusOK {
			return false
		}

		var descOut struct {
			FileSystems []struct {
				LifeCycleState string `json:"LifeCycleState"`
			} `json:"FileSystems"`
		}
		if err := json.Unmarshal(descRec.Body.Bytes(), &descOut); err != nil || len(descOut.FileSystems) == 0 {
			return false
		}

		return descOut.FileSystems[0].LifeCycleState == "available"
	}, 500*time.Millisecond, 20*time.Millisecond,
		"FS should transition to 'available' within 500ms")
}

// TestCreateFileSystem_CreationTokenIdempotency verifies identical params return 200
// and different params return 409, matching CreationToken idempotency semantics.
func TestCreateFileSystem_CreationTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		firstBody  map[string]any
		secondBody map[string]any
		name       string
		wantCode   int
	}{
		{
			name:       "identical params → 200 idempotent success",
			firstBody:  map[string]any{"CreationToken": "idem-1", "PerformanceMode": "generalPurpose"},
			secondBody: map[string]any{"CreationToken": "idem-1", "PerformanceMode": "generalPurpose"},
			wantCode:   http.StatusOK,
		},
		{
			name:      "different params → 409 conflict",
			firstBody: map[string]any{"CreationToken": "idem-2", "ThroughputMode": "bursting"},
			secondBody: map[string]any{
				"CreationToken":                "idem-2",
				"ThroughputMode":               "provisioned",
				"ProvisionedThroughputInMibps": 128,
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()

			rec1 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tc.firstBody)
			require.Equal(t, http.StatusCreated, rec1.Code, "first create: %s", rec1.Body.String())

			rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", tc.secondBody)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// TestDeleteFileSystem_BlockedByDependencies verifies DeleteFileSystem is blocked by
// mount targets or access points, and succeeds once the file system has no dependents.
func TestDeleteFileSystem_BlockedByDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupMount bool
		setupAP    bool
		wantCode   int
	}{
		{name: "blocked by mount target", setupMount: true, wantCode: http.StatusConflict},
		{name: "blocked by access point", setupAP: true, wantCode: http.StatusConflict},
		{name: "succeeds when empty", wantCode: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "parity-delete-"+tc.name)

			if tc.setupMount {
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-aabbccdd",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tc.setupAP {
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": fsID,
					"ClientToken":  "tok-ap-block",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			delRec := doREST(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
			assert.Equal(t, tc.wantCode, delRec.Code)
		})
	}
}

// TestDescribeFileSystems_PaginationMarker verifies DescribeFileSystems Marker/NextMarker
// pagination over the HTTP surface, including rejection of an unknown marker.
func TestDescribeFileSystems_PaginationMarker(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// paginate semantics: marker = first item of next page, skipped on resume.
	// 10 items, pageSize=3: page1=[0..2] marker=items[3],
	// page2=[4..6] marker=items[7], page3=[8..9] no marker.
	const total = 10
	for i := range total {
		createFS(t, h, fmt.Sprintf("parity-page-token-%02d", i))
	}

	rec1 := doREST(t, h, http.MethodGet, "/2015-02-01/file-systems?MaxItems=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	type fsPage struct {
		NextMarker  string `json:"NextMarker"`
		FileSystems []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"FileSystems"`
	}

	var page1 fsPage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.FileSystems, 3)
	require.NotEmpty(t, page1.NextMarker)

	rec2 := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?MaxItems=3&Marker="+page1.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 fsPage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.FileSystems, 3)
	require.NotEmpty(t, page2.NextMarker)

	rec3 := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?MaxItems=3&Marker="+page2.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var page3 fsPage
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &page3))
	require.Len(t, page3.FileSystems, 2, "last page has items[8..9]")
	assert.Empty(t, page3.NextMarker, "no more pages after last item")

	badRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems?Marker=nonexistent-marker-id", nil)
	assert.Equal(t, http.StatusBadRequest, badRec.Code, "invalid marker should return 400")
}
