package ec2_test

import (
	"net/http"
	"net/url"
	"regexp"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSnapshot verifies snapshot creation from a volume.
func TestCreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		volumeID    string
		setupVolume bool
		wantErr     bool
	}{
		{
			name:        "valid_snapshot",
			setupVolume: true,
			wantErr:     false,
		},
		{
			name:     "missing_volume_id",
			volumeID: "",
			wantErr:  true,
		},
		{
			name:     "non_existent_volume",
			volumeID: "vol-nonexistent",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			volumeID := tt.volumeID

			if tt.setupVolume {
				vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
				require.NoError(t, err)
				volumeID = vol.ID
			}

			snap, err := b.CreateSnapshot(volumeID, "test snapshot")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, snap.SnapshotID)
			assert.Equal(t, volumeID, snap.VolumeID)
			assert.Equal(t, "completed", snap.State)
			assert.Equal(t, "100%", snap.Progress)
		})
	}
}

// TestDescribeSnapshots verifies snapshot listing and filtering.

// TestDescribeSnapshots verifies snapshot listing and filtering.
func TestDescribeSnapshots(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	snap1, err := b.CreateSnapshot(vol.ID, "snap 1")
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "snap 2")
	require.NoError(t, err)

	// all
	all := b.DescribeSnapshots(nil)
	assert.Len(t, all, 2)

	// filtered
	filtered := b.DescribeSnapshots([]string{snap1.SnapshotID})
	require.Len(t, filtered, 1)
	assert.Equal(t, snap1.SnapshotID, filtered[0].SnapshotID)
}

// TestDeleteSnapshot verifies snapshot deletion.

// TestDeleteSnapshot verifies snapshot deletion.
func TestDeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "to be deleted")
	require.NoError(t, err)

	require.NoError(t, b.DeleteSnapshot(snap.SnapshotID))

	all := b.DescribeSnapshots(nil)
	assert.Empty(t, all)

	// delete again -> error
	assert.Error(t, b.DeleteSnapshot(snap.SnapshotID))
}

// TestSnapshotPersistence verifies Snapshot/Restore round-trip.

// TestSnapshotPersistence verifies Snapshot/Restore round-trip.
func TestSnapshotPersistence(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "persisted")
	require.NoError(t, err)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), data))

	snaps := b2.DescribeSnapshots(nil)
	assert.Len(t, snaps, 1)
	assert.Equal(t, "persisted", snaps[0].Description)
}

// TestCopyImage verifies image copying.

// TestReset_ClearsNewMaps verifies Reset clears snapshots and networkACLs.
func TestReset_ClearsNewMaps(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	_, err = b.CreateSnapshot(vol.ID, "test")
	require.NoError(t, err)

	_, err = b.CreateNetworkACL("vpc-default")
	require.NoError(t, err)

	b.Reset()

	assert.Empty(t, b.DescribeSnapshots(nil))
	assert.Empty(t, b.DescribeStoredNetworkAcls(nil))
}

// TestHTTP_CreateSnapshot verifies the HTTP handler for CreateSnapshot.

// TestHTTP_CreateSnapshot verifies the HTTP handler for CreateSnapshot.
func TestHTTP_CreateSnapshot(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create volume first
	rec := postForm(
		t,
		h,
		"Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp2",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Extract volume ID from response
	require.Contains(t, rec.Body.String(), "vol-")

	// Get the volume ID
	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	vols := b.DescribeVolumes(nil)
	require.NotEmpty(t, vols)

	rec = postForm(
		t,
		h,
		"Action=CreateSnapshot&Version=2016-11-15&VolumeId="+vols[0].ID+"&Description=test",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "snap-")
	assert.Contains(t, rec.Body.String(), "completed")
}

// TestHTTP_DescribeSnapshots verifies the HTTP handler for DescribeSnapshots.

// TestHTTP_DescribeSnapshots verifies the HTTP handler for DescribeSnapshots.
func TestHTTP_DescribeSnapshots(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=DescribeSnapshots&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeSnapshotsResponse")
}

// TestHTTP_DescribeSnapshots_Pagination verifies real (non-forged) NextToken
// round-tripping still works after switching DescribeSnapshots from a plain,
// unauthenticated integer offset to the same HMAC-signed opaque token used by
// DescribeInstances/DescribeImages/DescribeInstanceTypes.
func TestHTTP_DescribeSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)

	const totalSnapshots = 7

	created := make(map[string]bool, totalSnapshots)

	for range totalSnapshots {
		snap, cerr := b.CreateSnapshot(vol.ID, "page-test")
		require.NoError(t, cerr)
		created[snap.SnapshotID] = true
	}

	snapshotIDRe := regexp.MustCompile(`<snapshotId>(snap-[0-9a-f]+)</snapshotId>`)
	nextTokenRe := regexp.MustCompile(`<nextToken>([^<]+)</nextToken>`)

	seen := make(map[string]bool, totalSnapshots)
	nextToken := ""

	for {
		body := "Action=DescribeSnapshots&Version=2016-11-15&MaxResults=5"
		if nextToken != "" {
			body += "&NextToken=" + url.QueryEscape(nextToken)
		}

		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)

		for _, m := range snapshotIDRe.FindAllStringSubmatch(rec.Body.String(), -1) {
			seen[m[1]] = true
		}

		tokMatch := nextTokenRe.FindStringSubmatch(rec.Body.String())
		if tokMatch == nil {
			break
		}

		nextToken = tokMatch[1]
	}

	assert.Equal(t, created, seen, "pagination must traverse every snapshot exactly once across real tokens")
}
