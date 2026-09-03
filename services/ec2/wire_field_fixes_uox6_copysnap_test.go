package ec2_test

// uox6-copysnap: CopySnapshot declares Encrypted and KmsKeyId ("You can
// encrypt a copy of an unencrypted snapshot, but you cannot create an
// unencrypted copy of an encrypted snapshot." -- api_op_CopySnapshot.go,
// ec2@v1.319.1), but the handler never read either, so a client asking to
// encrypt a copy of an unencrypted source snapshot silently got an
// unencrypted copy back. The backend already inherits Encrypted/KmsKeyID
// from the source snapshot when the caller says nothing (that's the
// contingent default the SDK doc describes, and it was already correct);
// what was missing is honouring an explicit Encrypted=true override.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopySnapshot_ExplicitEncryptOverridesUnencryptedSource(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	vol, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(8),
	})
	require.NoError(t, err)
	require.False(t, aws.ToBool(vol.Encrypted))

	src, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)
	require.False(t, aws.ToBool(src.Encrypted))

	copied, err := client.CopySnapshot(t.Context(), &ec2sdk.CopySnapshotInput{
		SourceSnapshotId: src.SnapshotId,
		SourceRegion:     aws.String("us-east-1"),
		Encrypted:        aws.Bool(true),
		KmsKeyId:         aws.String("alias/uox6-copy-key"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
		SnapshotIds: []string{aws.ToString(copied.SnapshotId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.True(t, aws.ToBool(out.Snapshots[0].Encrypted))
	assert.Equal(t, "alias/uox6-copy-key", aws.ToString(out.Snapshots[0].KmsKeyId))
}

func TestCopySnapshot_EncryptedOmitted_InheritsSource(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	vol, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(8),
		Encrypted:        aws.Bool(true),
	})
	require.NoError(t, err)

	src, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)
	require.True(t, aws.ToBool(src.Encrypted))

	copied, err := client.CopySnapshot(t.Context(), &ec2sdk.CopySnapshotInput{
		SourceSnapshotId: src.SnapshotId,
		SourceRegion:     aws.String("us-east-1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
		SnapshotIds: []string{aws.ToString(copied.SnapshotId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.True(t, out.Snapshots[0].Encrypted != nil && *out.Snapshots[0].Encrypted)
	assert.Equal(t, aws.ToString(src.KmsKeyId), aws.ToString(out.Snapshots[0].KmsKeyId))
}

func TestCopySnapshot_ExplicitEncryptNoKmsKeyId_UsesDefaultEBSKey(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	vol, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(8),
	})
	require.NoError(t, err)

	src, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)

	copied, err := client.CopySnapshot(t.Context(), &ec2sdk.CopySnapshotInput{
		SourceSnapshotId: src.SnapshotId,
		SourceRegion:     aws.String("us-east-1"),
		Encrypted:        aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
		SnapshotIds: []string{aws.ToString(copied.SnapshotId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.Equal(t, "alias/aws/ebs", aws.ToString(out.Snapshots[0].KmsKeyId))
}
