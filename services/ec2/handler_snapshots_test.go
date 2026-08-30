package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCopySnapshot(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20, "")
	src, setupErr := b.CreateSnapshot(vol.ID, "original")
	require.NoError(t, setupErr)

	t.Run("creates copy with new ID", func(t *testing.T) { //nolint:paralleltest // existing issue.
		copiedSnap, err := b.CopySnapshot(src.SnapshotID, "copy description")
		require.NoError(t, err)
		assert.NotEqual(t, src.SnapshotID, copiedSnap.SnapshotID)
		assert.Equal(t, vol.ID, copiedSnap.VolumeID)
		assert.Equal(t, "copy description", copiedSnap.Description)
		assert.Equal(t, "completed", copiedSnap.State)
	})

	t.Run("unknown source returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CopySnapshot("snap-doesnotexist", "")
		require.Error(t, err)
	})
}

func TestCreateSnapshots(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, err := b.RunInstances("ami-parity-test", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instID := insts[0].ID

	v1, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
	v2, _ := b.CreateVolume("us-east-1a", "gp2", 20, "")
	_, err = b.AttachVolume(v1.ID, instID, "/dev/sdf")
	require.NoError(t, err)
	_, err = b.AttachVolume(v2.ID, instID, "/dev/sdg")
	require.NoError(t, err)

	t.Run("creates one snapshot per attached volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		snaps, snapErr := b.CreateSnapshots(instID, false, nil, "batch snap")
		require.NoError(t, snapErr)
		require.Len(t, snaps, 2)

		gotVolIDs := make(map[string]bool, len(snaps))
		for _, s := range snaps {
			assert.Equal(t, "completed", s.State)
			gotVolIDs[s.VolumeID] = true
		}
		assert.True(t, gotVolIDs[v1.ID])
		assert.True(t, gotVolIDs[v2.ID])
	})

	t.Run("empty instance id returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, snapErr := b.CreateSnapshots("", false, nil, "")
		require.Error(t, snapErr)
	})

	t.Run("unknown instance id returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, snapErr := b.CreateSnapshots("i-doesnotexist", false, nil, "")
		require.Error(t, snapErr)
	})
}

// ---- Snapshot block public access ---- //nolint:godot // existing issue.

// ---- Snapshot block public access ---- //nolint:godot // existing issue.
func TestSnapshotBlockPublicAccess(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default state is block-all-sharing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Equal(t, "block-all-sharing", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("enable sets block-new-sharing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableSnapshotBlockPublicAccess("block-new-sharing"))
		assert.Equal(t, "block-new-sharing", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("disable sets unblocked", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DisableSnapshotBlockPublicAccess()
		assert.Equal(t, "unblocked", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("invalid state returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.EnableSnapshotBlockPublicAccess("invalid"))
	})
}

func TestSnapshotTier(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20, "")
	snap, _ := b.CreateSnapshot(vol.ID, "tier test")

	t.Run("default tier is standard", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "standard", items[0].StorageTier)
	})

	t.Run("ModifySnapshotTier updates tier", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifySnapshotTier(snap.SnapshotID, "archive"))
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "archive", items[0].StorageTier)
	})

	t.Run("ResetSnapshotAttribute clears attribute", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetSnapshotAttribute(snap.SnapshotID))
	})

	t.Run( //nolint:paralleltest // existing issue.
		"ModifySnapshotTier unknown snapshot returns error",
		func(t *testing.T) {
			require.Error(t, b.ModifySnapshotTier("snap-missing", "archive"))
		},
	)
}

// ---- VPC/Subnet/SG ---- //nolint:godot // existing issue.

func TestHTTP_CopySnapshot(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
	snap, _ := b.CreateSnapshot(vol.ID, "src")

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":           []string{"CopySnapshot"},
		"SourceSnapshotId": []string{snap.SnapshotID},
		"Description":      []string{"copy"},
	})
	require.NoError(t, err)
}

func TestHTTP_GetSnapshotBlockPublicAccessState(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetSnapshotBlockPublicAccessState"},
	})
	require.NoError(t, err)
}

// ---- Snapshot locking ---- //nolint:godot // existing issue.
func TestSnapshotLocking(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
	snap, _ := b.CreateSnapshot(vol.ID, "lockable")

	t.Run("lock snapshot", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lock, err := b.LockSnapshot(snap.SnapshotID, "compliance", 90)
		require.NoError(t, err)
		assert.Equal(t, "compliance", lock.LockState)
		assert.Equal(t, 90, lock.LockDurationDays)
	})

	t.Run("describe returns lock", func(t *testing.T) { //nolint:paralleltest // existing issue.
		locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
		require.Len(t, locks, 1)
		assert.Equal(t, snap.SnapshotID, locks[0].SnapshotID)
	})

	t.Run("double lock returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.LockSnapshot(snap.SnapshotID, "governance", 0)
		require.Error(t, err)
	})

	t.Run("unlock snapshot", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.UnlockSnapshot(snap.SnapshotID))
		locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
		assert.Empty(t, locks)
	})

	t.Run("unlock non-locked snapshot returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.UnlockSnapshot(snap.SnapshotID))
	})
}

// ---- CopyVolumes ---- //nolint:godot // existing issue.

func TestHTTP_DescribeLockedSnapshots(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeLockedSnapshots"},
	})
	require.NoError(t, err)
}

// ---- Snapshot recycle bin ---- //nolint:godot // existing issue.
func TestSnapshotRecycleBin(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("list empty recycle bin", func(t *testing.T) { //nolint:paralleltest // existing issue.
		snaps := b.ListSnapshotsInRecycleBin(nil)
		assert.Empty(t, snaps)
	})

	t.Run("import snapshot creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.ImportSnapshot("test import")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ImportTaskID)
	})
}

func TestRestoreSnapshotTier(t *testing.T) { //nolint:paralleltest // existing issue.
	t.Run("restore from archive to standard", func(t *testing.T) {
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		vol, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
		snap, _ := b.CreateSnapshot(vol.ID, "tier test")
		_ = b.ModifySnapshotTier(snap.SnapshotID, "archive")

		require.NoError(t, b.RestoreSnapshotTier(snap.SnapshotID))
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "standard", items[0].StorageTier)
	})
}

// ---- Fast launch / snapshot restores ---- //nolint:godot // existing issue.

func TestFastSnapshotRestores(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("enable and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableFastSnapshotRestores([]string{"snap-abc"}, []string{"us-east-1a"}))
		items := b.DescribeFastSnapshotRestores()
		require.Len(t, items, 1)
		assert.Equal(t, "snap-abc", items[0].SnapshotID)
	})

	t.Run("disable removes entry", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableFastSnapshotRestores([]string{"snap-abc"}, []string{"us-east-1a"}))
		items := b.DescribeFastSnapshotRestores()
		assert.Empty(t, items)
	})
}

// ---- Instance utilities ---- //nolint:godot // existing issue.

func TestHTTP_DescribeFastSnapshotRestores(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeFastSnapshotRestores"},
	})
	require.NoError(t, err)
}

func TestHTTP_ListSnapshotsInRecycleBin(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"ListSnapshotsInRecycleBin"},
	})
	require.NoError(t, err)
}

// TestLockSnapshot_LockStateMatchesMode verifies that LockSnapshot returns a
// LockState equal to the requested mode ("compliance" or "governance"), matching AWS EC2
// behaviour. Previously the backend hardcoded "locked" for all modes.
func TestLockSnapshot_LockStateMatchesMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantLockState string
		lockMode      string
		durationDays  int
	}{
		{
			name:          "compliance_mode_returns_compliance_state",
			lockMode:      "compliance",
			durationDays:  90,
			wantLockState: "compliance",
		},
		{
			name:          "governance_mode_returns_governance_state",
			lockMode:      "governance",
			durationDays:  30,
			wantLockState: "governance",
		},
		{
			name:          "compliance_indefinite_returns_compliance_state",
			lockMode:      "compliance",
			durationDays:  0,
			wantLockState: "compliance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
			require.NoError(t, err)
			snap, err := b.CreateSnapshot(vol.ID, "audit-lock")
			require.NoError(t, err)

			lock, err := b.LockSnapshot(snap.SnapshotID, tt.lockMode, tt.durationDays)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLockState, lock.LockState,
				"LockState must match the requested lock mode")
			assert.Equal(t, snap.SnapshotID, lock.SnapshotID)

			// DescribeLockedSnapshots must also return the correct state.
			locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
			require.Len(t, locks, 1)
			assert.Equal(t, tt.wantLockState, locks[0].LockState)
		})
	}
}

// TestGetImageBlockPublicAccessState_DefaultUnblocked verifies that the
// account-level image block public access state defaults to "unblocked", matching AWS EC2
// behaviour. Previously the backend incorrectly defaulted to "block-new-sharing".

// TestHandlerDeleteSnapshot covers handleDeleteSnapshot.
func TestHandlerDeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create a volume and snapshot.
	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "test snap")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=DeleteSnapshot&Version=2016-11-15&SnapshotId="+snap.SnapshotID)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found.
	rec = postForm(t, h, "Action=DeleteSnapshot&Version=2016-11-15&SnapshotId=snap-notfound")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestSnapshotWireFields_EncryptedOwnerIDTags verifies CreateSnapshot,
// CreateSnapshots, CopySnapshot, and DescribeSnapshots all surface Encrypted,
// KmsKeyId, OwnerId, and TagSet on the wire.
func TestSnapshotWireFields_EncryptedOwnerIDTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "CreateSnapshot", action: "CreateSnapshot"},
		{name: "CreateSnapshots", action: "CreateSnapshots"},
		{name: "CopySnapshot", action: "CopySnapshot"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := ec2.NewHandler(b)

			vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
			require.NoError(t, err)
			require.NoError(t, b.SetVolumeEncryption(vol.ID, true, "alias/aws/ebs"))

			vals := url.Values{
				"Version":                         []string{"2016-11-15"},
				"TagSpecification.1.ResourceType": []string{"snapshot"},
				"TagSpecification.1.Tag.1.Key":    []string{"Purpose"},
				"TagSpecification.1.Tag.1.Value":  []string{"lineage-test"},
			}

			var snapID string

			switch tt.action {
			case "CreateSnapshot":
				vals["Action"] = []string{"CreateSnapshot"}
				vals["VolumeId"] = []string{vol.ID}
				resp, dispErr := ec2.ExportDispatch(h, vals)
				require.NoError(t, dispErr)
				assert.Contains(t, resp, "<encrypted>true</encrypted>")
				assert.Contains(t, resp, "<kmsKeyId>alias/aws/ebs</kmsKeyId>")
				assert.Contains(t, resp, "<ownerId>123456789012</ownerId>")
				assert.Contains(t, resp, "<key>Purpose</key>")
				assert.Contains(t, resp, "<value>lineage-test</value>")
				snapID = accuracyExtractXMLValue(resp, "snapshotId")

			case "CreateSnapshots":
				insts, instErr := b.RunInstances("ami-parity-test", "t3.micro", "", 1)
				require.NoError(t, instErr)
				_, attachErr := b.AttachVolume(vol.ID, insts[0].ID, "/dev/sdf")
				require.NoError(t, attachErr)

				vals["Action"] = []string{"CreateSnapshots"}
				vals["InstanceSpecification.InstanceId"] = []string{insts[0].ID}
				resp, dispErr := ec2.ExportDispatch(h, vals)
				require.NoError(t, dispErr)
				assert.Contains(t, resp, "<encrypted>true</encrypted>")
				assert.Contains(t, resp, "<ownerId>123456789012</ownerId>")
				assert.Contains(t, resp, "<key>Purpose</key>")
				snapID = accuracyExtractXMLValue(resp, "snapshotId")

			case "CopySnapshot":
				src, srcErr := b.CreateSnapshot(vol.ID, "src")
				require.NoError(t, srcErr)
				vals["Action"] = []string{"CopySnapshot"}
				vals["SourceSnapshotId"] = []string{src.SnapshotID}
				resp, dispErr := ec2.ExportDispatch(h, vals)
				require.NoError(t, dispErr)
				assert.Contains(t, resp, "<key>Purpose</key>")
				assert.Contains(t, resp, "<value>lineage-test</value>")
				snapID = accuracyExtractXMLValue(resp, "snapshotId")
			}

			require.NotEmpty(t, snapID)

			// DescribeSnapshots must independently reflect the same
			// Encrypted/OwnerId/TagSet, proving they are stored, not just
			// echoed back on the create response.
			descResp, err := ec2.ExportDispatch(h, url.Values{
				"Action":       []string{"DescribeSnapshots"},
				"Version":      []string{"2016-11-15"},
				"SnapshotId.1": []string{snapID},
			})
			require.NoError(t, err)
			assert.Contains(t, descResp, "<ownerId>123456789012</ownerId>")
			assert.Contains(t, descResp, "<key>Purpose</key>")
			assert.Contains(t, descResp, "<value>lineage-test</value>")
		})
	}
}

// TestHandlerDeregisterImage covers handleDeregisterImage.
