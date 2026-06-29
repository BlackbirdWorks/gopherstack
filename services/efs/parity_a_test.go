package efs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_SizeInBytes_IncludesStorageClassBreakdown verifies that DescribeFileSystems
// returns ValueInIA, ValueInStandard, and ValueInArchive inside SizeInBytes. Real AWS
// includes all three storage-class breakdown fields alongside Value.
func TestParity_SizeInBytes_IncludesStorageClassBreakdown(t *testing.T) {
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

// TestParity_ReplicationConfiguration_SourceOwnerInResponse verifies that
// CreateReplicationConfiguration and DescribeReplicationConfigurations return
// SourceFileSystemOwnerId. Real AWS includes the owner account ID.
func TestParity_ReplicationConfiguration_SourceOwnerInResponse(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "rc-owner-test",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rcRec := doREST(t, h, http.MethodPost,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/replication-configuration",
		map[string]any{
			"Destinations": []map[string]any{
				{"Region": "us-west-2"},
			},
		})
	require.Equal(t, http.StatusOK, rcRec.Code, "CreateReplicationConfiguration failed: %s", rcRec.Body.String())

	var rcOut struct {
		SourceFileSystemOwnerID string `json:"SourceFileSystemOwnerId"`
	}
	require.NoError(t, json.Unmarshal(rcRec.Body.Bytes(), &rcOut))

	assert.NotEmpty(t, rcOut.SourceFileSystemOwnerID,
		"SourceFileSystemOwnerId must be present in CreateReplicationConfiguration response")
}

// TestParity_ReplicationConfiguration_DestinationHasArnAndOwner verifies that
// destination entries in a replication configuration include FileSystemArn and OwnerId.
// Real AWS generates a destination file system and includes its ARN and owning account.
func TestParity_ReplicationConfiguration_DestinationHasArnAndOwner(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "rc-dest-arn-test",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rcRec := doREST(t, h, http.MethodPost,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/replication-configuration",
		map[string]any{
			"Destinations": []map[string]any{
				{"Region": "eu-west-1"},
			},
		})
	require.Equal(t, http.StatusOK, rcRec.Code, "CreateReplicationConfiguration failed: %s", rcRec.Body.String())

	var rcOut struct {
		Destinations []struct {
			FileSystemID  string `json:"FileSystemId"`
			FileSystemArn string `json:"FileSystemArn"`
			OwnerID       string `json:"OwnerId"`
			Status        string `json:"Status"`
		} `json:"Destinations"`
	}
	require.NoError(t, json.Unmarshal(rcRec.Body.Bytes(), &rcOut))
	require.Len(t, rcOut.Destinations, 1)

	d := rcOut.Destinations[0]
	assert.NotEmpty(t, d.FileSystemID, "destination FileSystemId must be assigned")
	assert.Contains(t, d.FileSystemArn, "arn:aws:elasticfilesystem:",
		"destination FileSystemArn must be a valid ARN")
	assert.NotEmpty(t, d.OwnerID, "destination OwnerId must be present")
	assert.Equal(t, "ENABLED", d.Status)
}

// TestParity_DescribeReplicationConfigurations_ByDestinationFileSystemID verifies that
// DescribeReplicationConfigurations with FileSystemId matching a destination (not source)
// returns the replication configuration. Real AWS matches both source and destination.
func TestParity_DescribeReplicationConfigurations_ByDestinationFileSystemID(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "rc-dest-lookup",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rcRec := doREST(t, h, http.MethodPost,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/replication-configuration",
		map[string]any{
			"Destinations": []map[string]any{
				{"Region": "ap-southeast-1"},
			},
		})
	require.Equal(t, http.StatusOK, rcRec.Code)

	var rcOut struct {
		Destinations []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"Destinations"`
	}
	require.NoError(t, json.Unmarshal(rcRec.Body.Bytes(), &rcOut))
	require.Len(t, rcOut.Destinations, 1)

	destFSID := rcOut.Destinations[0].FileSystemID
	require.NotEmpty(t, destFSID)

	// Now query using the destination FS ID.
	listRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems/replication-configurations?FileSystemId="+destFSID, nil)
	require.Equal(
		t, http.StatusOK, listRec.Code,
		"DescribeReplicationConfigurations by destination failed: %s", listRec.Body.String(),
	)

	var listOut struct {
		Replications []struct {
			SourceFileSystemID string `json:"SourceFileSystemId"`
		} `json:"Replications"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))

	assert.Len(t, listOut.Replications, 1,
		"DescribeReplicationConfigurations must return result when filtering by destination FS ID")
	assert.Equal(t, fsOut.FileSystemID, listOut.Replications[0].SourceFileSystemID)
}

// TestParity_CreateReplicationConfiguration_AssignsDestinationFileSystemID verifies that
// a destination entry without an explicit FileSystemId gets one assigned automatically.
// Real AWS always creates a destination file system.
func TestParity_CreateReplicationConfiguration_AssignsDestinationFileSystemID(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "rc-auto-dest-id",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rcRec := doREST(t, h, http.MethodPost,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/replication-configuration",
		map[string]any{
			"Destinations": []map[string]any{
				{"Region": "ca-central-1"},
			},
		})
	require.Equal(t, http.StatusOK, rcRec.Code)

	var rcOut struct {
		Destinations []struct {
			FileSystemID string `json:"FileSystemId"`
		} `json:"Destinations"`
	}
	require.NoError(t, json.Unmarshal(rcRec.Body.Bytes(), &rcOut))
	require.Len(t, rcOut.Destinations, 1)

	assert.NotEmpty(t, rcOut.Destinations[0].FileSystemID,
		"destination FileSystemId must be auto-assigned when not provided")
}

// TestParity_FileSystemPolicy_RoundTrip verifies that PutFileSystemPolicy stores the policy
// and DescribeFileSystemPolicy returns the exact same JSON. Real AWS returns the policy body
// verbatim in the Policy field.
func TestParity_FileSystemPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "policy-roundtrip",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"elasticfilesystem:ClientMount"}]}`

	putRec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/policy",
		map[string]any{"Policy": policy})
	require.Equal(t, http.StatusOK, putRec.Code, "PutFileSystemPolicy failed: %s", putRec.Body.String())

	descRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/policy", nil)
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeFileSystemPolicy failed: %s", descRec.Body.String())

	var descOut struct {
		Policy string `json:"Policy"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.JSONEq(t, policy, descOut.Policy,
		"DescribeFileSystemPolicy must return the stored policy verbatim")
}

// TestParity_BackupPolicy_EnabledRoundTrip verifies PutBackupPolicy stores the status
// and DescribeBackupPolicy returns it. Real AWS stores and returns the backup policy status.
func TestParity_BackupPolicy_EnabledRoundTrip(t *testing.T) {
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

// TestParity_BackupPolicy_InvalidStatusRejected verifies that PutBackupPolicy returns 400
// for an unrecognized status value, matching real AWS ValidationException.
func TestParity_BackupPolicy_InvalidStatusRejected(t *testing.T) {
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

// TestParity_LifecyclePolicy_InvalidTransitionRejected verifies that PutLifecycleConfiguration
// returns 400 for an invalid TransitionToIA value. Real AWS rejects unknown enum values.
func TestParity_LifecyclePolicy_InvalidTransitionRejected(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "lifecycle-invalid",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/lifecycle-configuration",
		map[string]any{
			"LifecyclePolicies": []map[string]any{
				{"TransitionToIA": "AFTER_999_DAYS"},
			},
		})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"PutLifecycleConfiguration with invalid TransitionToIA must return 400; body: %s", rec.Body.String())
}

// TestParity_MountTarget_SecurityGroupLimitEnforced verifies that CreateMountTarget returns
// a conflict error when more than 5 security groups are specified. Real AWS enforces this limit.
func TestParity_MountTarget_SecurityGroupLimitEnforced(t *testing.T) {
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

	assert.Equal(t, http.StatusConflict, rec.Code,
		"CreateMountTarget with >5 security groups must return 409; body: %s", rec.Body.String())
}

// TestParity_MountTarget_DuplicateSubnetRejected verifies that creating two mount targets for
// the same file system in the same subnet returns a conflict. Real AWS enforces one MT per
// file-system/subnet combination.
func TestParity_MountTarget_DuplicateSubnetRejected(t *testing.T) {
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

// TestParity_DeleteFileSystem_RejectedWhenMountTargetsExist verifies that DeleteFileSystem
// returns 409 FileSystemInUse when mount targets exist. Real AWS enforces this.
func TestParity_DeleteFileSystem_RejectedWhenMountTargetsExist(t *testing.T) {
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

// TestParity_AccessPoint_ClientTokenIdempotency verifies that CreateAccessPoint with the same
// ClientToken returns the same access point on repeat calls. Real AWS implements this
// idempotency guarantee.
func TestParity_AccessPoint_ClientTokenIdempotency(t *testing.T) {
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
