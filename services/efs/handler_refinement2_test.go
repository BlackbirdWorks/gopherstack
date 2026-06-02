package efs_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// --- Issue #2: CreationToken Idempotency ---

// TestRefinement2_CreationTokenIdempotency verifies identical args return 200, different args return 409.
func TestRefinement2_CreationTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		name       string
		first      efs.CreateFileSystemRequest
		second     efs.CreateFileSystemRequest
		wantSameFS bool
	}{
		{
			name:       "identical_token_and_mode_returns_existing",
			first:      efs.CreateFileSystemRequest{CreationToken: "tok", ThroughputMode: "bursting"},
			second:     efs.CreateFileSystemRequest{CreationToken: "tok", ThroughputMode: "bursting"},
			wantErrIs:  efs.ErrCreationTokenExists,
			wantSameFS: true,
		},
		{
			name:      "same_token_different_perf_mode_returns_conflict",
			first:     efs.CreateFileSystemRequest{CreationToken: "tok2", PerformanceMode: "generalPurpose"},
			second:    efs.CreateFileSystemRequest{CreationToken: "tok2", PerformanceMode: "maxIO"},
			wantErrIs: efs.ErrAlreadyExists,
		},
		{
			name: "same_token_different_encrypted_returns_conflict",
			first: efs.CreateFileSystemRequest{
				CreationToken: "tok3",
				Encrypted:     true,
			},
			second: efs.CreateFileSystemRequest{
				CreationToken: "tok3",
				Encrypted:     false,
			},
			wantErrIs: efs.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs1, err := b.CreateFileSystem(tt.first)
			require.NoError(t, err)

			fs2, err2 := b.CreateFileSystem(tt.second)
			require.ErrorIs(t, err2, tt.wantErrIs)

			if tt.wantSameFS {
				require.NotNil(t, fs2)
				assert.Equal(t, fs1.FileSystemID, fs2.FileSystemID)
			}
		})
	}
}

// TestRefinement2_CreationTokenIdempotency_HTTP verifies handler HTTP status codes.
func TestRefinement2_CreationTokenIdempotency_HTTP(t *testing.T) {
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
			name:       "different_perf_mode_returns_409",
			first:      map[string]any{"CreationToken": "http-tok2", "PerformanceMode": "generalPurpose"},
			second:     map[string]any{"CreationToken": "http-tok2", "PerformanceMode": "maxIO"},
			wantFirst:  http.StatusCreated,
			wantSecond: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			rec1 := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.first)
			assert.Equal(t, tt.wantFirst, rec1.Code)

			rec2 := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.second)
			assert.Equal(t, tt.wantSecond, rec2.Code)
		})
	}
}

// --- Issue #3: ProvisionedThroughputInMibps ---

// TestRefinement2_ProvisionedThroughput verifies provisioned throughput validation and persistence.
func TestRefinement2_ProvisionedThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		req       efs.CreateFileSystemRequest
		wantMibps float64
		wantErr   bool
	}{
		{
			name: "provisioned_with_valid_throughput",
			req: efs.CreateFileSystemRequest{
				CreationToken:            "prov-valid",
				ThroughputMode:           "provisioned",
				ProvisionedThroughputMib: 256,
			},
			wantMibps: 256,
		},
		{
			name: "provisioned_without_throughput_is_invalid",
			req: efs.CreateFileSystemRequest{
				CreationToken:  "prov-no-tp",
				ThroughputMode: "provisioned",
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "provisioned_throughput_below_minimum",
			req: efs.CreateFileSystemRequest{
				CreationToken:            "prov-low",
				ThroughputMode:           "provisioned",
				ProvisionedThroughputMib: 0.5,
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "provisioned_throughput_above_maximum",
			req: efs.CreateFileSystemRequest{
				CreationToken:            "prov-high",
				ThroughputMode:           "provisioned",
				ProvisionedThroughputMib: 2048,
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "non_provisioned_with_throughput_is_invalid",
			req: efs.CreateFileSystemRequest{
				CreationToken:            "prov-wrong-mode",
				ThroughputMode:           "bursting",
				ProvisionedThroughputMib: 100,
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "bursting_without_throughput_is_valid",
			req: efs.CreateFileSystemRequest{
				CreationToken:  "prov-bursting",
				ThroughputMode: "bursting",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(tt.req)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.wantMibps, fs.ProvisionedThroughputMib, 0.001)
			}
		})
	}
}

// TestRefinement2_ProvisionedThroughput_InResponse verifies the field appears in HTTP response.
func TestRefinement2_ProvisionedThroughput_InResponse(t *testing.T) {
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

			h := newRefinementHandler()
			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			resp := parseRefinementResp(t, rec)
			_, hasField := resp["ProvisionedThroughputInMibps"]
			assert.Equal(t, tt.wantInResp, hasField)

			if tt.wantInResp {
				assert.InDelta(t, tt.wantMibps, resp["ProvisionedThroughputInMibps"].(float64), 0.001)
			}
		})
	}
}

// --- Issue #4: Encrypted + KmsKeyID ---

// TestRefinement2_KmsKeyId verifies KmsKeyID auto-fill and validation.
func TestRefinement2_KmsKeyId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		name       string
		req        efs.CreateFileSystemRequest
		wantErr    bool
		wantKmsSet bool
	}{
		{
			name: "encrypted_without_kms_autofills_managed_key",
			req: efs.CreateFileSystemRequest{
				CreationToken: "kms-auto",
				Encrypted:     true,
			},
			wantKmsSet: true,
		},
		{
			name: "encrypted_with_custom_kms_persisted",
			req: efs.CreateFileSystemRequest{
				CreationToken: "kms-custom",
				Encrypted:     true,
				KmsKeyID:      "arn:aws:kms:us-east-1:123456789012:key/custom-key",
			},
			wantKmsSet: true,
		},
		{
			name: "unencrypted_with_kms_is_invalid",
			req: efs.CreateFileSystemRequest{
				CreationToken: "kms-invalid",
				Encrypted:     false,
				KmsKeyID:      "arn:aws:kms:us-east-1:123456789012:key/some-key",
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "unencrypted_without_kms_is_valid",
			req: efs.CreateFileSystemRequest{
				CreationToken: "kms-none",
				Encrypted:     false,
			},
			wantKmsSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(tt.req)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
				if tt.wantKmsSet {
					assert.NotEmpty(t, fs.KmsKeyID)
				} else {
					assert.Empty(t, fs.KmsKeyID)
				}
			}
		})
	}
}

// --- Issue #7: FileSystemProtection Round-Trip ---

// TestRefinement2_FileSystemProtection verifies protection is emitted in responses.
func TestRefinement2_FileSystemProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		protection     string
		wantProtection string
		wantHTTPStatus int
	}{
		{
			name:           "default_protection_is_disabled",
			protection:     "",
			wantHTTPStatus: http.StatusOK,
			wantProtection: "DISABLED",
		},
		{
			name:           "set_to_enabled",
			protection:     "ENABLED",
			wantHTTPStatus: http.StatusOK,
			wantProtection: "ENABLED",
		},
		{
			name:           "set_to_replicating",
			protection:     "REPLICATING",
			wantHTTPStatus: http.StatusOK,
			wantProtection: "REPLICATING",
		},
		{
			name:           "invalid_value_returns_400",
			protection:     "UNKNOWN",
			wantHTTPStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-prot-"+tt.name)

			if tt.protection != "" {
				rec := doRESTRefinement(t, h, http.MethodPut,
					"/2015-02-01/file-systems/"+fsID+"/protection",
					map[string]any{"ReplicationOverwriteProtection": tt.protection})
				assert.Equal(t, tt.wantHTTPStatus, rec.Code)

				if tt.wantHTTPStatus != http.StatusOK {
					return
				}
			}

			// Verify via describe.
			rec2 := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/file-systems/"+fsID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			resp2 := parseRefinementResp(t, rec2)
			fsList := resp2["FileSystems"].([]any)
			require.Len(t, fsList, 1)
			fsResp := fsList[0].(map[string]any)
			prot := fsResp["FileSystemProtection"].(map[string]any)
			assert.Equal(t, tt.wantProtection, prot["ReplicationOverwriteProtection"])
		})
	}
}

// --- Issue #8: MountTarget missing fields ---

// TestRefinement2_MountTargetFields verifies MountTarget response includes all required fields.
func TestRefinement2_MountTargetFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "all_fields_present"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-mt-fields-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-12345",
				"IpAddress":    "10.0.1.5",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.NotEmpty(t, resp["MountTargetId"])
			assert.NotEmpty(t, resp["NetworkInterfaceId"])
			assert.Equal(t, fsID, resp["FileSystemId"])
			assert.Equal(t, "subnet-12345", resp["SubnetId"])
			assert.Equal(t, "10.0.1.5", resp["IpAddress"])
			assert.NotEmpty(t, resp["OwnerId"])
			assert.NotEmpty(t, resp["LifeCycleState"])
			// VpcId and AZ fields are present (may be empty strings)
			_, hasVpcID := resp["VpcId"]
			_, hasAZName := resp["AvailabilityZoneName"]
			_, hasAZId := resp["AvailabilityZoneId"]
			assert.True(t, hasVpcID)
			assert.True(t, hasAZName)
			assert.True(t, hasAZId)
		})
	}
}

// --- Issue #9 & #27: CreateMountTarget SecurityGroups and max 5 limit ---

// TestRefinement2_MountTargetSecurityGroups verifies SecurityGroups parameter on create and max-5 quota.
func TestRefinement2_MountTargetSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		securityGroups []string
		wantHTTPStatus int
	}{
		{
			name:           "no_security_groups_ok",
			securityGroups: nil,
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:           "one_security_group_ok",
			securityGroups: []string{"sg-aaa111"},
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:           "five_security_groups_ok",
			securityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5"},
			wantHTTPStatus: http.StatusOK,
		},
		{
			name:           "six_security_groups_rejected",
			securityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
			wantHTTPStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-mt-sg-"+tt.name)

			body := map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-abc",
			}
			if tt.securityGroups != nil {
				body["SecurityGroups"] = tt.securityGroups
			}

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", body)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if tt.wantHTTPStatus == http.StatusOK && len(tt.securityGroups) > 0 {
				resp := parseRefinementResp(t, rec)
				sgs, ok := resp["SecurityGroups"].([]any)
				require.True(t, ok)
				assert.Len(t, sgs, len(tt.securityGroups))
			}
		})
	}
}

// TestRefinement2_ModifyMountTargetSecurityGroups_MaxQuota verifies the max-5 quota on modify.
func TestRefinement2_ModifyMountTargetSecurityGroups_MaxQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		securityGroups []string
		wantHTTPStatus int
	}{
		{
			name:           "replace_with_three",
			securityGroups: []string{"sg-x", "sg-y", "sg-z"},
			wantHTTPStatus: http.StatusNoContent,
		},
		{
			name:           "six_rejected",
			securityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
			wantHTTPStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-mtsgs-"+tt.name)

			mtRec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-abc",
			})
			require.Equal(t, http.StatusOK, mtRec.Code)
			mtID := parseRefinementResp(t, mtRec)["MountTargetId"].(string)

			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/mount-targets/"+mtID+"/security-groups",
				map[string]any{"SecurityGroups": tt.securityGroups})
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)
		})
	}
}

// --- Issue #10: One mount target per subnet ---

// TestRefinement2_MountTargetConflict verifies one-per-subnet enforcement.
func TestRefinement2_MountTargetConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subnets  []string
		wantLast int
	}{
		{
			name:     "same_subnet_twice_conflicts",
			subnets:  []string{"subnet-abc", "subnet-abc"},
			wantLast: http.StatusConflict,
		},
		{
			name:     "different_subnets_ok",
			subnets:  []string{"subnet-abc", "subnet-def"},
			wantLast: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-mt-conflict-"+tt.name)

			var lastCode int
			for _, sn := range tt.subnets {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     sn,
				})
				lastCode = rec.Code
			}
			assert.Equal(t, tt.wantLast, lastCode)
		})
	}
}

// --- Issue #11, #12, #13: AccessPoint PosixUser, RootDirectory, ClientToken ---

// TestRefinement2_AccessPointPosixUser verifies PosixUser is stored and returned.
func TestRefinement2_AccessPointPosixUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		posixUser *efs.PosixUser
		name      string
	}{
		{
			name: "posix_user_persisted",
			posixUser: &efs.PosixUser{
				UID:           1000,
				GID:           1001,
				SecondaryGids: []int64{2000, 2001},
			},
		},
		{
			name:      "no_posix_user_ok",
			posixUser: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-ap-posix-" + tt.name))
			require.NoError(t, err)

			ap, err := b.CreateAccessPoint(efs.CreateAccessPointRequest{
				FileSystemID: fs.FileSystemID,
				PosixUser:    tt.posixUser,
			})
			require.NoError(t, err)

			if tt.posixUser != nil {
				require.NotNil(t, ap.PosixUser)
				assert.Equal(t, tt.posixUser.UID, ap.PosixUser.UID)
				assert.Equal(t, tt.posixUser.GID, ap.PosixUser.GID)
				assert.Equal(t, tt.posixUser.SecondaryGids, ap.PosixUser.SecondaryGids)
			} else {
				assert.Nil(t, ap.PosixUser)
			}
		})
	}
}

// TestRefinement2_AccessPointRootDirectory verifies RootDirectory is stored and validated.
func TestRefinement2_AccessPointRootDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs     error
		rootDirectory *efs.RootDirectory
		name          string
		wantErr       bool
	}{
		{
			name: "root_path_no_creation_info_ok",
			rootDirectory: &efs.RootDirectory{
				Path: "/",
			},
		},
		{
			name: "non_root_path_with_creation_info_ok",
			rootDirectory: &efs.RootDirectory{
				Path: "/data",
				CreationInfo: &efs.CreationInfo{
					OwnerUID:    1000,
					OwnerGID:    1001,
					Permissions: "0755",
				},
			},
		},
		{
			name: "non_root_path_without_creation_info_invalid",
			rootDirectory: &efs.RootDirectory{
				Path: "/data",
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:          "nil_root_directory_ok",
			rootDirectory: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-ap-rd-" + tt.name))
			require.NoError(t, err)

			_, err = b.CreateAccessPoint(efs.CreateAccessPointRequest{
				FileSystemID:  fs.FileSystemID,
				RootDirectory: tt.rootDirectory,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement2_AccessPointClientTokenIdempotency verifies ClientToken deduplication.
func TestRefinement2_AccessPointClientTokenIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clientToken string
		wantSameAP  bool
	}{
		{
			name:        "same_client_token_returns_existing",
			clientToken: "ct-abc123",
			wantSameAP:  true,
		},
		{
			name:        "no_client_token_creates_new",
			clientToken: "",
			wantSameAP:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-ap-ct-" + tt.name))
			require.NoError(t, err)

			ap1, err := b.CreateAccessPoint(efs.CreateAccessPointRequest{
				FileSystemID: fs.FileSystemID,
				ClientToken:  tt.clientToken,
			})
			require.NoError(t, err)

			ap2, err := b.CreateAccessPoint(efs.CreateAccessPointRequest{
				FileSystemID: fs.FileSystemID,
				ClientToken:  tt.clientToken,
			})
			require.NoError(t, err)

			if tt.wantSameAP {
				assert.Equal(t, ap1.AccessPointID, ap2.AccessPointID)
				// Only one AP should exist.
				var aps []*efs.AccessPoint
				aps, _, err = b.DescribeAccessPoints(fs.FileSystemID, "", "", 0)
				require.NoError(t, err)
				assert.Len(t, aps, 1)
			} else {
				assert.NotEqual(t, ap1.AccessPointID, ap2.AccessPointID)
			}
		})
	}
}

// TestRefinement2_AccessPointResponse verifies ClientToken and PosixUser appear in HTTP response.
func TestRefinement2_AccessPointResponse(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-ap-resp-"+tt.name)
			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["FileSystemId"] = fsID

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/access-points", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)

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

// --- Issue #14: LifecyclePolicy enum validation ---

// TestRefinement2_LifecyclePolicyValidation verifies invalid enum values are rejected.
func TestRefinement2_LifecyclePolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		policy    efs.LifecyclePolicy
		name      string
		wantErr   bool
	}{
		{
			name:   "valid_transition_to_ia",
			policy: efs.LifecyclePolicy{TransitionToIA: "AFTER_30_DAYS"},
		},
		{
			name:   "valid_transition_to_primary",
			policy: efs.LifecyclePolicy{TransitionToPrimaryStorageClass: "AFTER_1_ACCESS"},
		},
		{
			name:      "invalid_transition_to_ia",
			policy:    efs.LifecyclePolicy{TransitionToIA: "AFTER_FOREVER"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "invalid_transition_to_primary",
			policy:    efs.LifecyclePolicy{TransitionToPrimaryStorageClass: "NEVER"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:   "all_none_valid",
			policy: efs.LifecyclePolicy{TransitionToIA: "NONE"},
		},
		{
			name:   "empty_policy_valid",
			policy: efs.LifecyclePolicy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-lp-" + tt.name))
			require.NoError(t, err)

			_, err = b.PutLifecycleConfiguration(fs.FileSystemID, []efs.LifecyclePolicy{tt.policy})

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement2_LifecyclePolicy_HTTPValidation verifies handler returns 400 for invalid enum.
func TestRefinement2_LifecyclePolicy_HTTPValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policies   []map[string]any
		wantStatus int
	}{
		{
			name:       "valid_policy_ok",
			policies:   []map[string]any{{"TransitionToIA": "AFTER_14_DAYS"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_ia_value_returns_400",
			policies:   []map[string]any{{"TransitionToIA": "INVALID_VALUE"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-lp-http-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/lifecycle-configuration",
				map[string]any{"LifecyclePolicies": tt.policies})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Issue #15: BackupPolicy enum validation ---

// TestRefinement2_BackupPolicyValidation verifies only valid status values are accepted.
func TestRefinement2_BackupPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		status    string
		wantErr   bool
	}{
		{name: "enabled_valid", status: "ENABLED"},
		{name: "disabled_valid", status: "DISABLED"},
		{name: "enabling_valid", status: "ENABLING"},
		{name: "disabling_valid", status: "DISABLING"},
		{name: "empty_invalid", status: "", wantErr: true, wantErrIs: efs.ErrValidation},
		{name: "unknown_status_invalid", status: "ACTIVE", wantErr: true, wantErrIs: efs.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-bp-val-" + tt.name))
			require.NoError(t, err)

			err = b.PutBackupPolicy(fs.FileSystemID, tt.status)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Issue #16: FileSystemPolicy JSON validation ---

// TestRefinement2_FileSystemPolicyValidation verifies policy must be valid JSON and ≤ 20 KB.
func TestRefinement2_FileSystemPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		policy    string
		wantErr   bool
	}{
		{
			name:   "valid_json_accepted",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name:      "invalid_json_rejected",
			policy:    `{not valid json}`,
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "empty_string_rejected",
			policy:    ``,
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-fsp-val-" + tt.name))
			require.NoError(t, err)

			err = b.PutFileSystemPolicy(fs.FileSystemID, tt.policy)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Issue #19: DeleteFileSystem rejects with FileSystemInUse ---

// TestRefinement2_DeleteFileSystem_FileSystemInUse verifies the correct error and HTTP status.
func TestRefinement2_DeleteFileSystem_FileSystemInUse(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "tok-del-inuse-"+tt.name)

			if tt.createMT {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-abc",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}
			if tt.createAP {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
					"FileSystemId": fsID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRESTRefinement(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if tt.wantErrCode != "" {
				resp := parseRefinementResp(t, rec)
				assert.Equal(t, tt.wantErrCode, resp["ErrorCode"])
			}
		})
	}
}

// --- Issue #22: Tag validation ---

// TestRefinement2_TagValidation verifies key length, value length, aws: prefix, and max count.
func TestRefinement2_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		tags      map[string]string
		name      string
		wantErr   bool
	}{
		{
			name: "valid_tags_accepted",
			tags: map[string]string{"env": "prod", "team": "eng"},
		},
		{
			name:      "aws_prefix_key_rejected",
			tags:      map[string]string{"aws:reserved": "value"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "empty_key_rejected",
			tags:      map[string]string{"": "value"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "value_too_long_rejected",
			tags:      map[string]string{"key": generateString(257)},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
				CreationToken: "tok-tagval-" + tt.name,
				Tags:          tt.tags,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement2_TagValidation_TagResource verifies validation on TagResource.
func TestRefinement2_TagValidation_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		tags      map[string]string
		name      string
		wantErr   bool
	}{
		{
			name: "valid_tags_accepted",
			tags: map[string]string{"k": "v"},
		},
		{
			name:      "aws_prefix_rejected",
			tags:      map[string]string{"aws:tag": "val"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-tagres-" + tt.name))
			require.NoError(t, err)

			err = b.TagResource(fs.FileSystemID, tt.tags)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Issue #23: Error body shape with x-amzn-ErrorType header ---

// TestRefinement2_ErrorBodyShape verifies x-amzn-ErrorType header is set on errors.
func TestRefinement2_ErrorBodyShape(t *testing.T) {
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
				h := newRefinementHandler()
				fsID := createFS(t, h, "tok-err-hdr-del")
				doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "sn-abc",
				})

				var bodyBytes []byte
				importJSON := func() {
					// noop, just to ensure rec is captured
				}
				_ = importJSON
				rec := doRESTRefinement(t, h, http.MethodDelete, "/2015-02-01/file-systems/"+fsID, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
				assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-Errortype"))
				_ = bodyBytes

				return
			}

			h := newRefinementHandler()
			rec := doRESTRefinement(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantErrorType, rec.Header().Get("X-Amzn-Errortype"))
		})
	}
}

// --- Issue #28: Throughput cooldown ---

// TestRefinement2_ThroughputCooldown verifies UpdateFileSystem enforces 24h cooldown.
func TestRefinement2_ThroughputCooldown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs     error
		name          string
		firstMode     string
		secondMode    string
		allowCooldown bool
		wantSecondErr bool
	}{
		{
			name:          "first_change_always_succeeds",
			firstMode:     "elastic",
			secondMode:    "",
			allowCooldown: false,
		},
		{
			name:          "second_change_within_cooldown_fails",
			firstMode:     "elastic",
			secondMode:    "bursting",
			allowCooldown: false,
			wantSecondErr: true,
			wantErrIs:     efs.ErrTooManyRequests,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-cooldown-" + tt.name))
			require.NoError(t, err)

			// First throughput change.
			_, err = b.UpdateFileSystem(fs.FileSystemID, efs.UpdateFileSystemRequest{
				ThroughputMode: tt.firstMode,
			})
			require.NoError(t, err)

			if tt.secondMode != "" {
				_, err = b.UpdateFileSystem(fs.FileSystemID, efs.UpdateFileSystemRequest{
					ThroughputMode: tt.secondMode,
				})

				if tt.wantSecondErr {
					require.ErrorIs(t, err, tt.wantErrIs)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

// --- Issue #29, #30: Pagination ---

// TestRefinement2_DescribeFileSystems_Pagination verifies Marker/NextMarker pagination.
func TestRefinement2_DescribeFileSystems_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		total     int
		maxItems  int
		wantFirst int
		wantNext  bool
	}{
		{
			name:      "all_items_fit_no_next",
			total:     3,
			maxItems:  5,
			wantFirst: 3,
			wantNext:  false,
		},
		{
			name:      "page_smaller_than_total",
			total:     5,
			maxItems:  2,
			wantFirst: 2,
			wantNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			for i := range tt.total {
				_, err := b.CreateFileSystem(fsReq("tok-page-" + tt.name + "-" + string(rune('a'+i))))
				require.NoError(t, err)
			}

			list, nextMarker, err := b.DescribeFileSystems("", "", "", tt.maxItems)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantFirst)

			if tt.wantNext {
				assert.NotEmpty(t, nextMarker)

				// Fetch second page.
				list2, _, err2 := b.DescribeFileSystems("", "", nextMarker, tt.maxItems)
				require.NoError(t, err2)
				assert.NotEmpty(t, list2)
			} else {
				assert.Empty(t, nextMarker)
			}
		})
	}
}

// TestRefinement2_DescribeMountTargets_Pagination verifies MaxItems/Marker pagination.
func TestRefinement2_DescribeMountTargets_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numMTs    int
		maxItems  int
		wantFirst int
		wantNext  bool
	}{
		{
			name:      "fits_in_one_page",
			numMTs:    2,
			maxItems:  5,
			wantFirst: 2,
			wantNext:  false,
		},
		{
			name:      "spans_multiple_pages",
			numMTs:    4,
			maxItems:  2,
			wantFirst: 2,
			wantNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-mt-page-" + tt.name))
			require.NoError(t, err)

			for i := range tt.numMTs {
				_, mtErr := b.CreateMountTarget(mtReq(fs.FileSystemID, "sn-"+string(rune('a'+i))))
				require.NoError(t, mtErr)
			}

			list, nextMarker, err := b.DescribeMountTargets(fs.FileSystemID, "", "", tt.maxItems)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantFirst)

			if tt.wantNext {
				assert.NotEmpty(t, nextMarker)
			} else {
				assert.Empty(t, nextMarker)
			}
		})
	}
}

// TestRefinement2_DescribeAccessPoints_Pagination verifies MaxResults/NextToken pagination.
func TestRefinement2_DescribeAccessPoints_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numAPs    int
		maxItems  int
		wantFirst int
		wantNext  bool
	}{
		{
			name:      "single_page",
			numAPs:    3,
			maxItems:  10,
			wantFirst: 3,
			wantNext:  false,
		},
		{
			name:      "two_pages",
			numAPs:    5,
			maxItems:  3,
			wantFirst: 3,
			wantNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-ap-page-" + tt.name))
			require.NoError(t, err)

			for range tt.numAPs {
				_, apErr := b.CreateAccessPoint(apReq(fs.FileSystemID))
				require.NoError(t, apErr)
			}

			list, nextToken, err := b.DescribeAccessPoints(fs.FileSystemID, "", "", tt.maxItems)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantFirst)

			if tt.wantNext {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

// --- Replication: protection flip on delete ---

// TestRefinement2_DeleteReplication_ProtectionFlip verifies source protection resets to DISABLED.
func TestRefinement2_DeleteReplication_ProtectionFlip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantProtection string
	}{
		{
			name:           "protection_resets_to_disabled_after_delete",
			wantProtection: "DISABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(fsReq("tok-repl-prot-" + tt.name))
			require.NoError(t, err)

			_, err = b.CreateReplicationConfiguration(fs.FileSystemID, []efs.ReplicationDestination{
				{Region: "us-west-2", Status: "ENABLED"},
			})
			require.NoError(t, err)

			// After create, source should be REPLICATING.
			list, _, err := b.DescribeFileSystems(fs.FileSystemID, "", "", 0)
			require.NoError(t, err)
			require.Len(t, list, 1)
			assert.Equal(t, "REPLICATING", list[0].ReplicationOverwriteProtection)

			// Delete the replication config.
			err = b.DeleteReplicationConfiguration(fs.FileSystemID)
			require.NoError(t, err)

			// Source protection should revert.
			list2, _, err := b.DescribeFileSystems(fs.FileSystemID, "", "", 0)
			require.NoError(t, err)
			require.Len(t, list2, 1)
			assert.Equal(t, tt.wantProtection, list2[0].ReplicationOverwriteProtection)
		})
	}
}

// --- tagsToEntries sort ---

// TestRefinement2_TagsToEntries_Sorted verifies tag entries are sorted alphabetically.
func TestRefinement2_TagsToEntries_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     map[string]any
		wantKeys []string
	}{
		{
			name:     "tags_sorted_alphabetically",
			tags:     map[string]any{"zebra": "z", "apple": "a", "mango": "m"},
			wantKeys: []string{"apple", "mango", "zebra"},
		},
		{
			name:     "single_tag_no_order_issue",
			tags:     map[string]any{"only": "one"},
			wantKeys: []string{"only"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			body := map[string]any{
				"CreationToken": "tok-tags-sort-" + tt.name,
				"Tags":          []map[string]any{},
			}
			tagEntries := make([]map[string]any, 0)
			for k, v := range tt.tags {
				tagEntries = append(tagEntries, map[string]any{"Key": k, "Value": v})
			}
			body["Tags"] = tagEntries

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			resp := parseRefinementResp(t, rec)
			rawTags, ok := resp["Tags"].([]any)
			require.True(t, ok)

			keys := make([]string, 0, len(rawTags))
			for _, rt := range rawTags {
				m := rt.(map[string]any)
				keys = append(keys, m["Key"].(string))
			}

			for i := 1; i < len(keys); i++ {
				assert.LessOrEqual(t, keys[i-1], keys[i], "tags not sorted at index %d", i)
			}
		})
	}
}

// --- UpdateFileSystem with ProvisionedThroughput ---

// TestRefinement2_UpdateFileSystem_ProvisionedThroughput verifies throughput updates are validated.
func TestRefinement2_UpdateFileSystem_ProvisionedThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		updateReq efs.UpdateFileSystemRequest
		wantErr   bool
	}{
		{
			name: "update_provisioned_throughput_ok",
			updateReq: efs.UpdateFileSystemRequest{
				ProvisionedThroughputMib: 512,
			},
		},
		{
			name: "update_provisioned_throughput_out_of_range",
			updateReq: efs.UpdateFileSystemRequest{
				ProvisionedThroughputMib: 2048,
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "provisioned_throughput_on_bursting_invalid",
			updateReq: efs.UpdateFileSystemRequest{
				ThroughputMode:           "bursting",
				ProvisionedThroughputMib: 100,
			},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			// Create a provisioned FS so we can update its throughput.
			fs, err := b.CreateFileSystem(efs.CreateFileSystemRequest{
				CreationToken:            "tok-upd-tp-" + tt.name,
				ThroughputMode:           "provisioned",
				ProvisionedThroughputMib: 100,
			})
			require.NoError(t, err)

			// Manually reset LastThroughputChange to bypass cooldown.
			fs.LastThroughputChange = time.Time{}
			b.AddFileSystemInternal(fs)

			_, err = b.UpdateFileSystem(fs.FileSystemID, tt.updateReq)

			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// generateString creates a string of the given length for test use.
func generateString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}

	return string(b)
}
