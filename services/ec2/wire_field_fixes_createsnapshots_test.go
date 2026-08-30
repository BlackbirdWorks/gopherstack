package ec2_test

// gopherstack-7uov: CreateSnapshots was broken for every real client. The
// handler never read InstanceSpecification.InstanceId -- the only required
// field on CreateSnapshotsInput (api_op_CreateSnapshots.go) -- had no real
// VolumeId wire parameter at all (the real API has none;
// serializers.go:72340's awsEc2query_serializeOpDocumentCreateSnapshotsInput
// only ever emits InstanceSpecification/Description/CopyTagsFromSource/
// DryRun/Location/OutpostArn/TagSpecification), and its ExcludeBootVolume
// fallback used that boolean's string value as a volume id. Confirmed
// failing against the unmodified handler: a real client never sends
// VolumeId.N, so the handler always fell through to "at least one VolumeId
// is required" (InvalidParameterValue) regardless of what was requested.
//
// The instance-to-volume relationship was already modelled (Volume.Attachment
// .InstanceID/Device, set by AttachVolume) but which attached volume is the
// *boot* volume was not tracked anywhere on Instance or Volume. This fix
// derives it from the instance's AMI RootDeviceName (via the existing
// lookupImageLocked helper) matched against the attaching Device -- when the
// AMI can't be resolved, no volume is treated as boot rather than guessing.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSnapshotsTestAMI is one of the seeded stubAMIs (RootDeviceName
// "/dev/xvda") so the boot-volume resolution path is exercised.
const createSnapshotsTestAMI = "ami-0c55b159cbfafe1f0"

func TestCreateSnapshots_RealWireKeys(t *testing.T) {
	t.Parallel()

	t.Run("creates_one_snapshot_per_attached_volume", func(t *testing.T) {
		t.Parallel()

		b, client := newTestBackendAndClient(t)

		insts, err := b.RunInstances(createSnapshotsTestAMI, "t3.micro", "", 1)
		require.NoError(t, err)
		instID := insts[0].ID

		v1, err := b.CreateVolume("us-east-1a", "gp2", 8, "")
		require.NoError(t, err)
		v2, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
		require.NoError(t, err)

		_, err = b.AttachVolume(v1.ID, instID, "/dev/xvda")
		require.NoError(t, err)
		_, err = b.AttachVolume(v2.ID, instID, "/dev/sdf")
		require.NoError(t, err)

		out, err := client.CreateSnapshots(t.Context(), &ec2sdk.CreateSnapshotsInput{
			InstanceSpecification: &types.InstanceSpecification{
				InstanceId: aws.String(instID),
			},
			Description: aws.String("crash-consistent set"),
		})
		require.NoError(t, err)
		require.Len(t, out.Snapshots, 2)

		gotVolIDs := make(map[string]bool, len(out.Snapshots))
		for _, s := range out.Snapshots {
			require.NotNil(t, s.VolumeId)
			gotVolIDs[*s.VolumeId] = true
			assert.Equal(t, types.SnapshotStateCompleted, s.State)
		}
		assert.True(t, gotVolIDs[v1.ID])
		assert.True(t, gotVolIDs[v2.ID])
	})

	t.Run("excludes_boot_volume", func(t *testing.T) {
		t.Parallel()

		b, client := newTestBackendAndClient(t)

		insts, err := b.RunInstances(createSnapshotsTestAMI, "t3.micro", "", 1)
		require.NoError(t, err)
		instID := insts[0].ID

		root, err := b.CreateVolume("us-east-1a", "gp2", 8, "")
		require.NoError(t, err)
		data, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
		require.NoError(t, err)

		_, err = b.AttachVolume(root.ID, instID, "/dev/xvda")
		require.NoError(t, err)
		_, err = b.AttachVolume(data.ID, instID, "/dev/sdf")
		require.NoError(t, err)

		out, err := client.CreateSnapshots(t.Context(), &ec2sdk.CreateSnapshotsInput{
			InstanceSpecification: &types.InstanceSpecification{
				InstanceId:        aws.String(instID),
				ExcludeBootVolume: aws.Bool(true),
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Snapshots, 1)
		require.NotNil(t, out.Snapshots[0].VolumeId)
		assert.Equal(t, data.ID, *out.Snapshots[0].VolumeId)
	})

	t.Run("excludes_data_volume_ids", func(t *testing.T) {
		t.Parallel()

		b, client := newTestBackendAndClient(t)

		insts, err := b.RunInstances(createSnapshotsTestAMI, "t3.micro", "", 1)
		require.NoError(t, err)
		instID := insts[0].ID

		root, err := b.CreateVolume("us-east-1a", "gp2", 8, "")
		require.NoError(t, err)
		data1, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
		require.NoError(t, err)
		data2, err := b.CreateVolume("us-east-1a", "gp2", 30, "")
		require.NoError(t, err)

		_, err = b.AttachVolume(root.ID, instID, "/dev/xvda")
		require.NoError(t, err)
		_, err = b.AttachVolume(data1.ID, instID, "/dev/sdf")
		require.NoError(t, err)
		_, err = b.AttachVolume(data2.ID, instID, "/dev/sdg")
		require.NoError(t, err)

		out, err := client.CreateSnapshots(t.Context(), &ec2sdk.CreateSnapshotsInput{
			InstanceSpecification: &types.InstanceSpecification{
				InstanceId:           aws.String(instID),
				ExcludeDataVolumeIds: []string{data1.ID},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Snapshots, 2)

		gotVolIDs := make(map[string]bool, len(out.Snapshots))
		for _, s := range out.Snapshots {
			gotVolIDs[*s.VolumeId] = true
		}
		assert.True(t, gotVolIDs[root.ID])
		assert.True(t, gotVolIDs[data2.ID])
		assert.False(t, gotVolIDs[data1.ID])
	})

	t.Run("unattached_instance_returns_error", func(t *testing.T) {
		t.Parallel()

		b, client := newTestBackendAndClient(t)

		insts, err := b.RunInstances(createSnapshotsTestAMI, "t3.micro", "", 1)
		require.NoError(t, err)

		_, err = client.CreateSnapshots(t.Context(), &ec2sdk.CreateSnapshotsInput{
			InstanceSpecification: &types.InstanceSpecification{
				InstanceId: aws.String(insts[0].ID),
			},
		})
		require.Error(t, err)
	})
}
