package efs_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

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
		{
			// AWS EFS's SecurityGroupLimitExceeded error has httpStatusCode 400 in the
			// service model (botocore efs/service-2.json), not 409 -- it's a client
			// input-validation error (too many security groups), not a resource conflict.
			name: "create_mount_target_too_many_security_groups_returns_400",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "sg-limit-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId":   fsID,
					"SubnetId":       "subnet-abc",
					"SecurityGroups": []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
				})
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
				assert.Equal(t, "SecurityGroupLimitExceeded", parseResp(t, rec2)["ErrorCode"])
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

// TestCreateMountTarget_SubnetIDRequired verifies SubnetId is required,
// matching AWS EFS behavior.
func TestCreateMountTarget_SubnetIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "with_subnet_id_accepted",
			body:       map[string]any{"SubnetId": "subnet-abc123"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "without_subnet_id_rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_subnet_id_rejected",
			body:       map[string]any{"SubnetId": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "mt-subnet-"+tt.name)

			body := map[string]any{"FileSystemId": fsID}
			maps.Copy(body, tt.body)

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				resp := parseResp(t, rec)
				assert.Contains(t, resp["ErrorCode"], "BadRequest")
			}
		})
	}
}

// TestMountTargetArn verifies that CreateMountTarget and DescribeMountTargets
// responses include MountTargetArn, matching the AWS EFS MountTargetDescription shape.
func TestMountTargetArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		via  string // "create" or "describe"
	}{
		{name: "create_response_includes_arn", via: "create"},
		{name: "describe_response_includes_arn", via: "describe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "mt-arn-"+tt.name)

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-aabbcc",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var mtARN string
			if tt.via == "create" {
				resp := parseResp(t, rec)
				mtARN, _ = resp["MountTargetArn"].(string)
			} else {
				rec2 := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets", nil)
				require.Equal(t, http.StatusOK, rec2.Code)
				mts := parseResp(t, rec2)["MountTargets"].([]any)
				require.Len(t, mts, 1)
				mtARN, _ = mts[0].(map[string]any)["MountTargetArn"].(string)
			}

			assert.NotEmpty(t, mtARN)
			assert.Contains(t, mtARN, "mount-target/fsmt-")
		})
	}
}

// TestDescribeMountTargets_AccessPointIdFilter_HTTP verifies that passing ?AccessPointId=
// to DescribeMountTargets returns mount targets for the file system the access point belongs
// to, matching real AWS EFS behavior.
func TestDescribeMountTargets_AccessPointIdFilter_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErr    string
		wantCount  int
		wantStatus int
		hasMT      bool
	}{
		{
			name:       "access_point_with_mount_target_returns_it",
			hasMT:      true,
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "access_point_without_mount_target_returns_empty",
			hasMT:      false,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "nonexistent_access_point_returns_404",
			wantStatus: http.StatusNotFound,
			wantErr:    "AccessPointNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()

			if tt.wantStatus == http.StatusNotFound {
				rec := doREST(
					t, h, http.MethodGet,
					"/2015-02-01/mount-targets?AccessPointId=fsap-notexist",
					nil,
				)
				assert.Equal(t, tt.wantStatus, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantErr, resp["ErrorCode"])

				return
			}

			fsID := createFS(t, h, "mt-ap-filter-"+tt.name)

			// Create access point on the file system.
			rec := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
				"FileSystemId": fsID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			apID := parseResp(t, rec)["AccessPointId"].(string)

			if tt.hasMT {
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-1122",
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			}

			rec3 := doREST(
				t, h, http.MethodGet,
				"/2015-02-01/mount-targets?AccessPointId="+apID,
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec3.Code)

			mts := parseResp(t, rec3)["MountTargets"].([]any)
			assert.Len(t, mts, tt.wantCount)

			if tt.wantCount > 0 {
				mt := mts[0].(map[string]any)
				assert.Equal(t, fsID, mt["FileSystemId"])
				assert.NotEmpty(t, mt["MountTargetArn"])
			}
		})
	}
}

// TestSortedDescribeMountTargets verifies sorted mount targets.
func TestSortedDescribeMountTargets(t *testing.T) {
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
			fsID := createFS(t, h, "tok-mt-sort-"+tt.name)

			for i := range tt.count {
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/mount-targets",
					map[string]any{
						"FileSystemId": fsID,
						"SubnetId":     "sn-" + string(rune('a'+i)),
					},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
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

// TestMountTargetFields verifies MountTarget response includes all required fields.
func TestMountTargetFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "all_fields_present"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-mt-fields-"+tt.name)

			rec := doREST(
				t,
				h,
				http.MethodPost,
				"/2015-02-01/mount-targets",
				map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-12345",
					"IpAddress":    "10.0.1.5",
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
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

// TestMountTargetSecurityGroups verifies SecurityGroups parameter on create and max-5 quota.
func TestMountTargetSecurityGroups(t *testing.T) {
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
			// SecurityGroupLimitExceeded has httpStatusCode 400 in the AWS EFS service
			// model (botocore efs/service-2.json), not 409.
			name:           "six_security_groups_rejected",
			securityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
			wantHTTPStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-mt-sg-"+tt.name)

			body := map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-abc",
			}
			if tt.securityGroups != nil {
				body["SecurityGroups"] = tt.securityGroups
			}

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", body)
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)

			if tt.wantHTTPStatus == http.StatusOK && len(tt.securityGroups) > 0 {
				resp := parseResp(t, rec)
				sgs, ok := resp["SecurityGroups"].([]any)
				require.True(t, ok)
				assert.Len(t, sgs, len(tt.securityGroups))
			}
		})
	}
}

// TestMountTargetConflict verifies one-per-subnet enforcement.
func TestMountTargetConflict(t *testing.T) {
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

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-mt-conflict-"+tt.name)

			var lastCode int
			for _, sn := range tt.subnets {
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/mount-targets",
					map[string]any{
						"FileSystemId": fsID,
						"SubnetId":     sn,
					},
				)
				lastCode = rec.Code
			}
			assert.Equal(t, tt.wantLast, lastCode)
		})
	}
}

// TestMountTarget_SecurityGroupLimitEnforced verifies that CreateMountTarget returns
// SecurityGroupLimitExceeded (HTTP 400 per the AWS EFS service model -- botocore
// efs/service-2.json lists httpStatusCode 400 for this error, not 409) when more than 5
// security groups are specified. Real AWS enforces this limit.
func TestMountTarget_SecurityGroupLimitEnforced(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "mt-sg-limit",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"SubnetId":     "subnet-aabbccdd",
		"SecurityGroups": []string{
			"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6",
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateMountTarget with >5 security groups must return 400; body: %s", rec.Body.String())
}

// TestMountTarget_DuplicateSubnetRejected verifies that creating two mount targets for
// the same file system in the same subnet returns a conflict. Real AWS enforces one MT per
// file-system/subnet combination.
func TestMountTarget_DuplicateSubnetRejected(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "mt-dup-subnet",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	firstRec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"SubnetId":     "subnet-duplicate",
	})
	require.Equal(t, http.StatusOK, firstRec.Code, "first CreateMountTarget failed: %s", firstRec.Body.String())

	secondRec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsOut.FileSystemID,
		"SubnetId":     "subnet-duplicate",
	})

	assert.Equal(t, http.StatusConflict, secondRec.Code,
		"duplicate CreateMountTarget in same subnet must return 409; body: %s", secondRec.Body.String())
}

// TestCreateMountTarget_PopulatesVpcAndAZ verifies CreateMountTarget populates VpcId,
// AvailabilityZoneName, and AvailabilityZoneId in the response.
func TestCreateMountTarget_PopulatesVpcAndAZ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subnetID string
	}{
		{name: "standard subnet format", subnetID: "subnet-abc12345"},
		{name: "non-standard subnet", subnetID: "custom-subnet-01"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "parity-mt-vpc-"+tc.subnetID)

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     tc.subnetID,
			})
			require.Equal(t, http.StatusOK, rec.Code, "CreateMountTarget: %s", rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			vpcID, _ := out["VpcId"].(string)
			azName, _ := out["AvailabilityZoneName"].(string)
			azID, _ := out["AvailabilityZoneId"].(string)

			assert.True(t, len(vpcID) > 4 && vpcID[:4] == "vpc-",
				"VpcId %q should start with 'vpc-'", vpcID)
			assert.NotEmpty(t, azName, "AvailabilityZoneName must be non-empty")
			assert.NotEmpty(t, azID, "AvailabilityZoneId must be non-empty")
			assert.True(t, len(azName) > 0 && azName[len(azName)-1] >= 'a' && azName[len(azName)-1] <= 'z',
				"AvailabilityZoneName %q should end with a letter", azName)
		})
	}
}

// TestCreateMountTarget_VpcIDStablePerSubnet verifies VpcID is stable per subnet.
func TestCreateMountTarget_VpcIDStablePerSubnet(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID1 := createFS(t, h, "parity-vpc-stable-1")
	fsID2 := createFS(t, h, "parity-vpc-stable-2")

	const subnetID = "subnet-deadbeef"

	rec1 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID1,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var mt1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &mt1))

	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID2,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var mt2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &mt2))

	assert.Equal(t, mt1["VpcId"], mt2["VpcId"],
		"same subnet should yield same VpcId across different file systems")
}

// TestCreateMountTarget_OneZone_InheritsAZ verifies a One-Zone FS's mount target
// inherits the AZ from the file system.
func TestCreateMountTarget_OneZone_InheritsAZ(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	const az = "us-east-1c"

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken":        "parity-onezone-fs",
		"AvailabilityZoneName": az,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fsOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fsOut))
	fsID := fsOut["FileSystemId"].(string)

	mtRec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-cafebabe",
	})
	require.Equal(t, http.StatusOK, mtRec.Code)

	var mt map[string]any
	require.NoError(t, json.Unmarshal(mtRec.Body.Bytes(), &mt))

	assert.Equal(t, az, mt["AvailabilityZoneName"],
		"mount target should inherit FS AvailabilityZoneName for One Zone storage class")
}

// TestCreateMountTarget_SubnetConflict verifies O(1) subnet conflict detection.
func TestCreateMountTarget_SubnetConflict(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID := createFS(t, h, "parity-subnet-conflict")

	const subnetID = "subnet-11223344"

	rec1 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnetID,
	})
	require.Equal(t, http.StatusOK, rec1.Code, "first mount target should succeed")

	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnetID,
	})
	assert.Equal(t, http.StatusConflict, rec2.Code, "duplicate subnet in same FS should conflict")

	rec3 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     "subnet-different0",
	})
	assert.Equal(t, http.StatusOK, rec3.Code, "different subnet in same FS should succeed")
}

// TestDescribeMountTargets_Pagination_HTTP verifies DescribeMountTargets pagination
// over the HTTP surface, including no-overlap between pages.
func TestDescribeMountTargets_Pagination_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID := createFS(t, h, "parity-mt-page")

	const total = 5
	for i := range total {
		rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
			"FileSystemId": fsID,
			"SubnetId":     fmt.Sprintf("subnet-%08d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code, "mount target %d: %s", i, rec.Body.String())
	}

	rec1 := doREST(t, h, http.MethodGet,
		"/2015-02-01/mount-targets?FileSystemId="+fsID+"&MaxItems=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	type mtPage struct {
		NextMarker   string `json:"NextMarker"`
		MountTargets []struct {
			MountTargetID string `json:"MountTargetId"`
		} `json:"MountTargets"`
	}

	var pg1 mtPage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &pg1))
	require.Len(t, pg1.MountTargets, 3)
	require.NotEmpty(t, pg1.NextMarker)

	rec2 := doREST(t, h, http.MethodGet,
		"/2015-02-01/mount-targets?FileSystemId="+fsID+"&MaxItems=3&Marker="+pg1.NextMarker, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var pg2 mtPage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &pg2))
	// 5 items, pageSize=3: page1=[0,1,2] marker=items[3], page2 resumes AT items[3] => [3,4].
	assert.Len(t, pg2.MountTargets, 2)
	assert.Empty(t, pg2.NextMarker)

	seen := make(map[string]bool)
	for _, mt := range pg1.MountTargets {
		seen[mt.MountTargetID] = true
	}

	for _, mt := range pg2.MountTargets {
		assert.False(t, seen[mt.MountTargetID], "mount target %s appears in both pages", mt.MountTargetID)
	}

	assert.Len(t, seen, 3, "sanity: page1 recorded 3 distinct ids before the union check")
	total2 := len(pg1.MountTargets) + len(pg2.MountTargets)
	assert.Equal(t, total, total2, "pagination must not lose or duplicate items across pages")
}

// TestDeleteMountTarget_CleansSubnetIndex verifies DeleteMountTarget removes the
// subnet index entry so the same subnet can be re-used.
func TestDeleteMountTarget_CleansSubnetIndex(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID := createFS(t, h, "parity-mt-idx-cleanup")

	const subnet = "subnet-deadcafe"

	rec1 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnet,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var mt1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &mt1))
	mtID := mt1["MountTargetId"].(string)

	delRec := doREST(t, h, http.MethodDelete, "/2015-02-01/mount-targets/"+mtID, nil)
	require.Equal(t, http.StatusNoContent, delRec.Code)

	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId": fsID,
		"SubnetId":     subnet,
	})
	assert.Equal(t, http.StatusOK, rec2.Code,
		"re-create in same subnet after delete should succeed: %s", rec2.Body.String())
}
